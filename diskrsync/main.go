package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dop251/diskrsync"
	"github.com/dop251/spgz"

	flag "github.com/spf13/pflag"
	"github.com/vbauerster/mpb/v7"
)

const (
	modeSource = iota
	modeTarget
)

type options struct {
	sshFlags   string
	noCompress bool
	verbose    bool
}

type proc interface {
	Start(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error, calcPl, syncPl diskrsync.ProgressListener) error
	IsLocal() bool
}

type localProc struct {
	p    string
	mode int
	opts *options
}

type remoteProc struct {
	p    string
	mode int
	opts *options
	host string
	cmd  *exec.Cmd
}

// used to prevent output to stderr while the progress bars are active
var bufStderr = bufferedOut{
	w: os.Stderr,
}

func usage() {
	_, _ = fmt.Fprintf(os.Stderr, "Usage: %s [flags] <src> <dst>\nsrc and dst is [[user@]host:]path\n\nFlags:\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(2)
}

func split(arg string) (host, path string) {
	a := strings.SplitN(arg, ":", 2)
	if len(a) == 2 {
		host = a[0]
		path = a[1]
		if path == "" {
			path = "./"
		}
	} else {
		path = arg
	}
	return
}

func createProc(arg string, mode int, opts *options) proc {
	host, path := split(arg)
	if host != "" {
		return createRemoteProc(host, path, mode, opts)
	}
	return createLocalProc(path, mode, opts)
}

func createRemoteProc(host, path string, mode int, opts *options) proc {
	return &remoteProc{
		host: host,
		p:    path,
		mode: mode,
		opts: opts,
	}
}

func createLocalProc(p string, mode int, opts *options) proc {
	return &localProc{
		p:    p,
		mode: mode,
		opts: opts,
	}
}

func (p *localProc) Start(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error, calcPl, syncPl diskrsync.ProgressListener) error {
	go p.run(cmdReader, cmdWriter, errChan, calcPl, syncPl)
	return nil
}

func (p *localProc) IsLocal() bool {
	return true
}

func (p *localProc) run(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error, calcPl, syncPl diskrsync.ProgressListener) {
	var err error
	if p.mode == modeSource {
		err = doSource(p.p, cmdReader, cmdWriter, p.opts, calcPl, syncPl)
	} else {
		err = doTarget(p.p, cmdReader, cmdWriter, p.opts, calcPl, syncPl)
	}

	cerr := cmdWriter.Close()
	if err == nil {
		err = cerr
	}
	errChan <- err
}

func (p *remoteProc) pipeCopy(dst io.WriteCloser, src io.Reader) {
	_, err := io.Copy(dst, src)
	if err != nil {
		log.Printf("pipe copy failed: %v", err)
	}
	err = dst.Close()
	if err != nil {
		log.Printf("close failed after pipe copy: %v", err)
	}
}

func (p *remoteProc) Start(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error, calcPl, syncPl diskrsync.ProgressListener) error {
	cmd := exec.Command("ssh")
	p.cmd = cmd
	args := cmd.Args

	if p.opts.sshFlags != "" {
		flags := strings.Split(p.opts.sshFlags, " ")
		args = append(args, flags...)
	}

	args = append(args, p.host, os.Args[0])

	if p.mode == modeSource {
		args = append(args, "--source")
	} else {
		args = append(args, "--target")
		if p.opts.noCompress {
			args = append(args, " --no-compress")
		}
	}
	if p.opts.verbose && calcPl == nil {
		args = append(args, " --verbose")
	}
	if calcPl == nil {
		cmd.Stderr = os.Stderr
	} else {
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return err
		}
		args = append(args, "--calc-progress")
		if syncPl != nil {
			args = append(args, "--sync-progress")
		}

		go func() {
			r := bufio.NewReader(stderr)
			readStart := func() (string, error) {
				for {
					line, err := r.ReadString('\n')
					if name := strings.TrimPrefix(line, "[Start "); name != line && len(name) > 1 {
						return name[:len(name)-2], nil
					}
					if len(line) > 0 {
						_, werr := bufStderr.Write([]byte(line))
						if werr != nil {
							return "", werr
						}
					}
					if err != nil {
						return "", err
					}
				}
			}
			pr := &progressReader{
				r:  r,
				w:  &bufStderr,
				pl: calcPl,
			}
			name, err := readStart()
			if err != nil {
				return
			}
			if name == "calc" {
				err := pr.read()
				if err != nil {
					return
				}
				if syncPl != nil {
					name, err = readStart()
					if err != nil {
						return
					}
				}
			}
			if syncPl != nil && name == "sync" {
				pr.pl = syncPl
				err = pr.read()
				if err != nil {
					return
				}
			}
			_, _ = io.Copy(os.Stderr, r)
		}()
	}

	args = append(args, p.p)
	cmd.Args = args
	cmd.Stdin = cmdReader

	r, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	err = cmd.Start()
	if err != nil {
		return err
	}
	go p.run(cmdWriter, r, errChan)
	return nil
}

func (p *remoteProc) IsLocal() bool {
	return false
}

func (p *remoteProc) run(w io.WriteCloser, r io.Reader, errChan chan error) {
	p.pipeCopy(w, r)
	errChan <- p.cmd.Wait()
}

func doSource(p string, cmdReader io.Reader, cmdWriter io.WriteCloser, opts *options, calcPl, syncPl diskrsync.ProgressListener) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}

	defer f.Close()

	var src io.ReadSeeker

	// Try to open as an spgz file
	sf, err := spgz.NewFromFile(f, os.O_RDONLY)
	if err != nil {
		if err != spgz.ErrInvalidFormat {
			return err
		}
		src = f
	} else {
		src = sf
	}

	size, err := src.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	_, err = src.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = diskrsync.Source(src, size, cmdReader, cmdWriter, true, opts.verbose, calcPl, syncPl)
	cerr := cmdWriter.Close()
	if err == nil {
		err = cerr
	}
	return err
}

func doTarget(p string, cmdReader io.Reader, cmdWriter io.WriteCloser, opts *options, calcPl, syncPl diskrsync.ProgressListener) (err error) {
	var w spgz.SparseFile
	useReadBuffer := false

	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return
	}

	if info.Mode()&(os.ModeDevice|os.ModeCharDevice) != 0 {
		w = spgz.NewSparseFileWithoutHolePunching(f)
		useReadBuffer = true
	} else if !opts.noCompress {
		sf, err := spgz.NewFromFileSize(f, os.O_RDWR|os.O_CREATE, diskrsync.DefTargetBlockSize)
		if err != nil {
			if err != spgz.ErrInvalidFormat {
				if err == spgz.ErrPunchHoleNotSupported {
					err = fmt.Errorf("target does not support compression. Try with -no-compress option (error was '%v')", err)
				}
				_ = f.Close()
				return err
			}
		} else {
			w = &diskrsync.FixingSpgzFileWrapper{SpgzFile: sf}
		}
	}

	if w == nil {
		w = spgz.NewSparseFileWithFallback(f)
		useReadBuffer = true
	}

	defer func() {
		cerr := w.Close()
		if err == nil {
			err = cerr
		}
	}()

	size, err := w.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	_, err = w.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	err = diskrsync.Target(w, size, cmdReader, cmdWriter, useReadBuffer, opts.verbose, calcPl, syncPl)
	cerr := cmdWriter.Close()
	if err == nil {
		err = cerr
	}

	return
}

func doCmd(opts *options) (err error) {
	src := createProc(flag.Arg(0), modeSource, opts)

	path := flag.Arg(1)
	if _, p := split(path); strings.HasSuffix(p, "/") {
		path += filepath.Base(flag.Arg(0))
	}

	dst := createProc(path, modeTarget, opts)

	srcErrChan := make(chan error, 1)
	dstErrChan := make(chan error, 1)

	srcReader, dstWriter := io.Pipe()
	dstReader, srcWriter := io.Pipe()

	sr := &diskrsync.CountingReader{Reader: srcReader}
	sw := &diskrsync.CountingWriteCloser{WriteCloser: srcWriter}

	var (
		p                 *mpb.Progress
		cancel            func()
		srcCalcPl, syncPl diskrsync.ProgressListener
	)

	if opts.verbose {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		defer func() {
			if cancel != nil {
				cancel()
			}
			bufStderr.Release()
		}()
		p = mpb.NewWithContext(ctx)
		if src.IsLocal() && !dst.IsLocal() {
			syncPl = newSyncProgressBarListener(p)
		}
		srcCalcPl = newCalcProgressBarListener(p, "Source Checksums")
		log.SetOutput(&bufStderr)
	}
	err = src.Start(sr, sw, srcErrChan, srcCalcPl, syncPl)

	if err != nil {
		return fmt.Errorf("could not start source: %w", err)
	}

	var dstCalcPl, dstSyncPl diskrsync.ProgressListener

	if opts.verbose {
		dstCalcPl = newCalcProgressBarListener(p, "Target Checksums")
		if syncPl == nil {
			dstSyncPl = newSyncProgressBarListener(p)
		}
	}
	err = dst.Start(dstReader, dstWriter, dstErrChan, dstCalcPl, dstSyncPl)

	if err != nil {
		return fmt.Errorf("could not start target: %w", err)
	}

L:
	for srcErrChan != nil || dstErrChan != nil {
		select {
		case dstErr := <-dstErrChan:
			if dstErr != nil {
				if !errors.Is(dstErr, io.EOF) {
					err = fmt.Errorf("target error: %w", dstErr)
					break L
				}
			}
			dstErrChan = nil
		case srcErr := <-srcErrChan:
			if srcErr != nil {
				if !errors.Is(srcErr, io.EOF) {
					err = fmt.Errorf("source error: %w", srcErr)
					break L
				}
			}
			srcErrChan = nil
		}
	}

	if cancel != nil {
		if err == nil {
			p.Wait()
		}
		cancel()
		cancel = nil
	}

	if opts.verbose {
		log.Printf("Read: %d, wrote: %d\n", sr.Count(), sw.Count())
	}
	return
}

func main() {
	// These flags are for the remote command mode, not to be used directly.
	var sourceMode = flag.Bool("source", false, "Source mode")
	var targetMode = flag.Bool("target", false, "Target mode")
	var calcProgress = flag.Bool("calc-progress", false, "Write calc progress")
	var syncProgress = flag.Bool("sync-progress", false, "Write sync progress")
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		f.Hidden = true
	})

	var opts options

	flag.StringVar(&opts.sshFlags, "ssh-flags", "", "SSH flags")
	flag.BoolVar(&opts.noCompress, "no-compress", false, "Store target as a raw file")
	flag.BoolVar(&opts.verbose, "verbose", false, "Print statistics, progress, and some debug info")

	flag.Parse()

	if *sourceMode || *targetMode {
		var calcPl, syncPl diskrsync.ProgressListener

		if *calcProgress {
			calcPl = &progressWriter{
				name: "calc",
				w:    os.Stderr,
			}
		}

		if *syncProgress {
			syncPl = &progressWriter{
				name: "sync",
				w:    os.Stderr,
			}
		}

		if *sourceMode {
			err := doSource(flag.Arg(0), os.Stdin, os.Stdout, &opts, calcPl, syncPl)
			if err != nil {
				log.Fatalf("Source failed: %s", err.Error())
			}
		} else {
			err := doTarget(flag.Arg(0), os.Stdin, os.Stdout, &opts, calcPl, syncPl)
			if err != nil {
				log.Fatalf("Target failed: %s", err.Error())
			}
		}
	} else {
		if flag.Arg(0) == "" || flag.Arg(1) == "" {
			usage()
		}
		err := doCmd(&opts)
		if err != nil {
			log.Fatal(err)
		}
	}

}
