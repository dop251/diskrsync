package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dop251/diskrsync"
	"github.com/dop251/spgz"
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
	Start(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error) error
}

type localProc struct {
	p    string
	mode int
	opts *options
}

type remoteProc struct {
	cmd *exec.Cmd
}

func usage() {
	_, _ = fmt.Fprintf(os.Stderr, "Usage: %s [--ssh-flags=\"...\"] [--no-compress] [--verbose] <src> <dst>\nsrc and dst is [[user@]host:]path\n", os.Args[0])
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

func createProc(arg string, mode int, opts *options) (proc, error) {
	host, path := split(arg)
	if host != "" {
		return createRemoteProc(host, path, mode, opts)
	}
	return createLocalProc(path, mode, opts)
}

func createRemoteProc(host, path string, mode int, opts *options) (proc, error) {
	var m string
	if mode == modeSource {
		m = "--source"
	} else {
		m = "--target"
		if opts.noCompress {
			m += " --no-compress"
		}
	}
	if opts.verbose {
		m += " --verbose"
	}

	args := make([]string, 1, 8)
	args[0] = "ssh"

	if opts.sshFlags != "" {
		flags := strings.Split(opts.sshFlags, " ")
		args = append(args, flags...)
	}

	args = append(args, host, os.Args[0], m, path)
	cmd := exec.Command("ssh")
	cmd.Args = args

	return &remoteProc{
		cmd: cmd,
	}, nil
}

func createLocalProc(p string, mode int, opts *options) (proc, error) {
	pr := &localProc{
		p:    p,
		mode: mode,
		opts: opts,
	}

	return pr, nil
}

func (p *localProc) Start(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error) error {
	go p.run(cmdReader, cmdWriter, errChan)
	return nil
}

func (p *localProc) run(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error) {
	var err error
	if p.mode == modeSource {
		err = doSource(p.p, cmdReader, cmdWriter, p.opts)
	} else {
		err = doTarget(p.p, cmdReader, cmdWriter, p.opts)
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

func (p *remoteProc) Start(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error) error {
	p.cmd.Stderr = os.Stderr
	p.cmd.Stdin = cmdReader

	r, err := p.cmd.StdoutPipe()
	if err != nil {
		return err
	}

	err = p.cmd.Start()
	if err != nil {
		return err
	}
	go p.run(cmdWriter, r, errChan)
	return nil
}

func (p *remoteProc) run(w io.WriteCloser, r io.Reader, errChan chan error) {
	p.pipeCopy(w, r)
	errChan <- p.cmd.Wait()
}

func doSource(p string, cmdReader io.Reader, cmdWriter io.WriteCloser, opts *options) error {
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

	err = diskrsync.Source(src, size, cmdReader, cmdWriter, true, opts.verbose)
	cerr := cmdWriter.Close()
	if err == nil {
		err = cerr
	}
	return err
}

func doTarget(p string, cmdReader io.Reader, cmdWriter io.WriteCloser, opts *options) (err error) {
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

	err = diskrsync.Target(w, size, cmdReader, cmdWriter, useReadBuffer, opts.verbose)
	cerr := cmdWriter.Close()
	if err == nil {
		err = cerr
	}

	return
}

func doCmd(opts *options) (err error) {
	src, err := createProc(flag.Arg(0), modeSource, opts)
	if err != nil {
		return fmt.Errorf("could not create source: %w", err)
	}

	path := flag.Arg(1)
	if _, p := split(path); strings.HasSuffix(p, "/") {
		path += filepath.Base(flag.Arg(0))
	}

	dst, err := createProc(path, modeTarget, opts)
	if err != nil {
		return fmt.Errorf("could not create target: %w", err)
	}

	srcErrChan := make(chan error, 1)
	dstErrChan := make(chan error, 1)

	srcReader, dstWriter := io.Pipe()
	dstReader, srcWriter := io.Pipe()

	sr := &diskrsync.CountingReader{Reader: srcReader}
	sw := &diskrsync.CountingWriteCloser{WriteCloser: srcWriter}

	if opts.verbose {
		err = src.Start(sr, sw, srcErrChan)
	} else {
		err = src.Start(srcReader, srcWriter, srcErrChan)
	}

	if err != nil {
		return fmt.Errorf("could not start source: %w", err)
	}

	err = dst.Start(dstReader, dstWriter, dstErrChan)
	if err != nil {
		return fmt.Errorf("could not start target: %w", err)
	}

L:
	for srcErrChan != nil || dstErrChan != nil {
		select {
		case dstErr := <-dstErrChan:
			if dstErr != nil {
				err = fmt.Errorf("target error: %w", dstErr)
				break L
			}
			dstErrChan = nil
		case srcErr := <-srcErrChan:
			if srcErr != nil {
				err = fmt.Errorf("source error: %w", srcErr)
				break L
			}
			srcErrChan = nil
		}
	}

	if opts.verbose {
		log.Printf("Read: %d, wrote: %d\n", sr.Count(), sw.Count())
	}
	return
}

func main() {
	var sourceMode = flag.Bool("source", false, "Source mode")
	var targetMode = flag.Bool("target", false, "Target mode")

	var opts options

	flag.StringVar(&opts.sshFlags, "ssh-flags", "", "SSH flags")
	flag.BoolVar(&opts.noCompress, "no-compress", false, "Store target as a raw file")
	flag.BoolVar(&opts.verbose, "verbose", false, "Print statistics and some debug info")

	flag.Parse()

	if *sourceMode {
		if opts.verbose {
			log.Println("Running source")
		}
		err := doSource(flag.Arg(0), os.Stdin, os.Stdout, &opts)
		if err != nil {
			log.Fatalf("Source failed: %s", err.Error())
		}
	} else if *targetMode {
		if opts.verbose {
			log.Println("Running target")
		}
		err := doTarget(flag.Arg(0), os.Stdin, os.Stdout, &opts)
		if err != nil {
			log.Fatalf("Target failed: %s", err.Error())
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
