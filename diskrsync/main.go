package main

import (
	"github.com/dop251/diskrsync"
	"github.com/dop251/spgz"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	fmt.Fprintf(os.Stderr, "Usage: %s [--ssh-flags=\"...\"] [--no-compress] [--verbose] <src> <dst>\nsrc and dst is [[user@]host:]path\n", os.Args[0])
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
	if p.mode == modeSource {
		errChan <- doSource(p.p, cmdReader, cmdWriter, p.opts)
	} else {
		errChan <- doTarget(p.p, cmdReader, cmdWriter, p.opts)
	}

	cmdWriter.Close()
}

func (p *remoteProc) Start(cmdReader io.Reader, cmdWriter io.WriteCloser, errChan chan error) error {
	p.cmd.Stdout = cmdWriter
	p.cmd.Stderr = os.Stderr

	w, err := p.cmd.StdinPipe()
	if err != nil {
		return err
	}

	go func() {
		io.Copy(w, cmdReader)
		w.Close()
	}()

	err = p.cmd.Start()
	if err != nil {
		return err
	}
	go p.run(cmdWriter, errChan)
	return nil
}

func (p *remoteProc) run(writer io.Closer, errChan chan error) {
	err := p.cmd.Wait()
	writer.Close()
	errChan <- err
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

	size, err := src.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	_, err = src.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	err = diskrsync.Source(src, size, cmdReader, cmdWriter, true, opts.verbose)
	cmdWriter.Close()
	return err
}

func doTarget(p string, cmdReader io.Reader, cmdWriter io.WriteCloser, opts *options) error {
	var w io.ReadWriteSeeker
	useBuffer := false

	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	if !opts.noCompress {
		sf, err := spgz.NewFromFileSize(f, os.O_RDWR|os.O_CREATE, diskrsync.DefTargetBlockSize)
		if err != nil {
			if err != spgz.ErrInvalidFormat {
				f.Close()
				return err
			}
		} else {
			defer sf.Close()
			w = sf
		}
	}

	if w == nil {
		sf := spgz.NewSparseWriter(spgz.NewSparseFile(f))
		defer sf.Close()
		w = sf
		useBuffer = true
	}

	size, err := w.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	_, err = w.Seek(0, os.SEEK_SET)

	if err != nil {
		return err
	}

	err = diskrsync.Target(w, size, cmdReader, cmdWriter, useBuffer, opts.verbose)
	cmdWriter.Close()

	return err
}

func doCmd(opts *options) error {
	src, err := createProc(flag.Arg(0), modeSource, opts)
	if err != nil {
		return err
	}

	path := flag.Arg(1)
	if _, p := split(path); strings.HasSuffix(p, "/") {
		path += filepath.Base(flag.Arg(0))
	}

	dst, err := createProc(path, modeTarget, opts)
	if err != nil {
		return err
	}

	errChan := make(chan error, 1)

	srcReader, dstWriter := io.Pipe()
	dstReader, srcWriter := io.Pipe()

	sr := &diskrsync.CountingReader{Reader: srcReader}
	sw := &diskrsync.CountingWriteCloser{WriteCloser: srcWriter}

	if opts.verbose {
		src.Start(sr, sw, errChan)
	} else {
		src.Start(srcReader, srcWriter, errChan)
	}

	dst.Start(dstReader, dstWriter, errChan)
	err = <-errChan
	if err != nil {
		return err
	}
	err = <-errChan
	if opts.verbose {
		log.Printf("Read: %d, wrote: %d\n", sr.Count(), sw.Count())
	}
	return err
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
			log.Fatalf(err.Error())
		}
	}

}
