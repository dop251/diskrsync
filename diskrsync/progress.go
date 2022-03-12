package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dop251/diskrsync"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

type bufferedOut struct {
	sync.Mutex
	buf bytes.Buffer
	w   io.Writer

	blocked bool
}

func (b *bufferedOut) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	if !b.blocked {
		return b.w.Write(p)
	}
	return b.buf.Write(p)
}

func (b *bufferedOut) Block() {
	b.Lock()
	b.blocked = true
	b.Unlock()
}

func (b *bufferedOut) Release() {
	b.Lock()
	defer b.Unlock()
	if b.blocked {
		_, _ = b.w.Write(b.buf.Bytes())
		b.buf.Reset()
		b.blocked = false
	}
}

type calcProgressBar struct {
	p          *mpb.Progress
	bar        *mpb.Bar
	name       string
	lastUpdate time.Time
}

func newCalcProgressBarListener(p *mpb.Progress, name string) *calcProgressBar {
	return &calcProgressBar{
		p:    p,
		name: name,
	}
}

func (pb *calcProgressBar) Start(size int64) {
	bufStderr.Block()
	pb.bar = pb.p.New(size,
		mpb.BarStyle().Rbound("|").Padding(" "),
		mpb.BarPriority(1),
		mpb.PrependDecorators(
			decor.Name(pb.name, decor.WC{C: decor.DidentRight | decor.DextraSpace | decor.DSyncWidth}),
			decor.CountersKibiByte("% .2f / % .2f", decor.WCSyncSpace),
		),
		mpb.AppendDecorators(
			decor.OnComplete(
				decor.EwmaETA(decor.ET_STYLE_GO, 60, decor.WC{W: 4}), "done",
			),
			decor.Name(" ] "),
			decor.EwmaSpeed(decor.UnitKiB, "% .2f", 60, decor.WCSyncSpace),
		),
	)
	pb.lastUpdate = time.Now()
}

func (pb *calcProgressBar) Update(pos int64) {
	pb.bar.SetCurrent(pos)
	now := time.Now()
	pb.bar.DecoratorEwmaUpdate(now.Sub(pb.lastUpdate))
	pb.lastUpdate = now
}

type syncProgressBar struct {
	p   *mpb.Progress
	bar *mpb.Bar
}

func newSyncProgressBarListener(p *mpb.Progress) *syncProgressBar {
	return &syncProgressBar{
		p: p,
	}
}

func (pb *syncProgressBar) Start(size int64) {
	const name = "Sync"
	bufStderr.Block()
	pb.bar = pb.p.New(size,
		mpb.BarStyle().Padding(" "),
		mpb.BarPriority(2),
		mpb.PrependDecorators(
			decor.Name(name, decor.WC{C: decor.DidentRight | decor.DextraSpace | decor.DSyncWidth}),
			decor.CountersKibiByte("% .2f / % .2f", decor.WCSyncSpace),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
		),
	)
}

func (pb *syncProgressBar) Update(pos int64) {
	pb.bar.SetCurrent(pos)
}

type progressWriter struct {
	name string
	w    io.Writer

	size      int64
	lastWrite time.Time
}

func (pw *progressWriter) Start(size int64) {
	pw.size = size
	pw.lastWrite = time.Now()
	fmt.Fprintf(pw.w, "[Start %s]\nSize: %d\n", pw.name, size)
}

func (pw *progressWriter) Update(pos int64) {
	if pw.size <= 0 {
		return
	}
	now := time.Now()
	if pos >= pw.size || now.Sub(pw.lastWrite) >= 250*time.Millisecond {
		fmt.Fprintf(pw.w, "Update: %d\n", pos)
		pw.lastWrite = now
	}
}

type progressReader struct {
	r  *bufio.Reader
	w  io.Writer
	pl diskrsync.ProgressListener
}

func (pr *progressReader) read() error {
	var size int64
	for {
		str, err := pr.r.ReadString('\n')
		if s := strings.TrimPrefix(str, "Size: "); s != str && len(s) > 0 {
			sz, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
			if err == nil {
				pr.pl.Start(sz)
				size = sz
				break
			}
		}
		if len(str) > 0 {
			_, werr := pr.w.Write([]byte(str))
			if werr != nil {
				return werr
			}
		}
		if err != nil {
			return err
		}
	}
	if size <= 0 {
		return nil
	}
	for {
		str, err := pr.r.ReadString('\n')
		if s := strings.TrimPrefix(str, "Update: "); s != str && len(s) > 0 {
			pos, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
			if err == nil {
				pr.pl.Update(pos)
				if pos >= size {
					break
				}
				continue
			}
		}
		if len(str) > 0 {
			_, werr := pr.w.Write([]byte(str))
			if werr != nil {
				return werr
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}
