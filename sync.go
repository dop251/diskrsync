package diskrsync

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"math"

	"github.com/dop251/spgz"

	"golang.org/x/crypto/blake2b"
)

const (
	hdrMagic = "BSNC0002"
)

const (
	hashSize = 64

	DefTargetBlockSize = 128 * 1024
)

const (
	cmdHole byte = iota
	cmdBlock
	cmdEqual
	cmdNotEqual
)

var (
	ErrInvalidFormat = errors.New("invalid data format")
)

type hashPool []hash.Hash

type workCtx struct {
	buf  []byte
	n    *node
	hash hash.Hash

	avail, hashReady chan struct{}
}

type node struct {
	buf    [hashSize]byte
	parent *node
	idx    int

	children []*node

	size int

	hash hash.Hash
	sum  []byte
}

type tree struct {
	root      *node
	size      int64
	reader    io.ReadSeeker
	useBuffer bool
}

type base struct {
	t         tree
	buf       []byte
	cmdReader io.Reader
	cmdWriter io.Writer
}

type source struct {
	base
	reader io.ReadSeeker
}

type target struct {
	base
	writer *batchingWriter
}

// Accumulates successive writes into a large buffer so that the writes into the underlying spgz.SpgzFile
// cover compressed blocks completely, so they are not read and unpacked before writing.
type batchingWriter struct {
	writer  spgz.SparseFile
	maxSize int

	offset   int64
	holeSize int64
	buf      []byte
}

func (w *batchingWriter) Flush() error {
	if w.holeSize > 0 {
		err := w.writer.PunchHole(w.offset, w.holeSize)
		if err == nil {
			w.offset += w.holeSize
			w.holeSize = 0
		}
		return err
	}
	if len(w.buf) == 0 {
		return nil
	}
	n, err := w.writer.WriteAt(w.buf, w.offset)
	if err != nil {
		return err
	}
	w.buf = w.buf[:0]
	w.offset += int64(n)
	return nil
}

func (w *batchingWriter) prepareWrite() error {
	if w.holeSize > 0 {
		err := w.Flush()
		if err != nil {
			return err
		}
	}
	if cap(w.buf) < w.maxSize {
		buf := make([]byte, w.maxSize)
		copy(buf, w.buf)
		w.buf = buf[:len(w.buf)]
	}
	return nil
}

func (w *batchingWriter) Write(p []byte) (int, error) {
	if err := w.prepareWrite(); err != nil {
		return 0, err
	}
	written := 0
	for len(p) > 0 {
		if len(p) >= w.maxSize && len(w.buf) == 0 {
			residue := len(p) % w.maxSize
			n, err := w.writer.WriteAt(p[:len(p)-residue], w.offset)
			written += n
			w.offset += int64(n)
			if err != nil {
				return written, err
			}
			p = p[n:]
		} else {
			n := copy(w.buf[len(w.buf):w.maxSize], p)
			w.buf = w.buf[:len(w.buf)+n]
			if len(w.buf) == w.maxSize {
				n1, err := w.writer.WriteAt(w.buf, w.offset)
				w.offset += int64(n1)
				n2 := n1 - (len(w.buf) - n)
				w.buf = w.buf[:0]
				if n2 < 0 {
					n2 = 0
				}
				written += n2
				if err != nil {
					return written, err
				}
			} else {
				written += n
			}
			p = p[n:]
		}
	}

	return written, nil
}

func (w *batchingWriter) ReadFrom(src io.Reader) (int64, error) {
	if err := w.prepareWrite(); err != nil {
		return 0, err
	}

	var read int64
	for {
		n, err := src.Read(w.buf[len(w.buf):w.maxSize])
		read += int64(n)
		w.buf = w.buf[:len(w.buf)+n]
		if err == io.EOF {
			return read, nil
		}
		if err != nil {
			return read, err
		}
		if len(w.buf) == w.maxSize {
			err = w.Flush()
			if err != nil {
				return read, err
			}
		}
	}
}

func (w *batchingWriter) WriteHole(size int64) error {
	if w.holeSize == 0 {
		err := w.Flush()
		if err != nil {
			return err
		}
	}
	w.holeSize += size
	return nil
}

func (w *batchingWriter) Seek(offset int64, whence int) (int64, error) {
	var o int64
	if w.holeSize > 0 {
		o = w.offset + w.holeSize
	} else {
		o = w.offset + int64(len(w.buf))
	}
	switch whence {
	case io.SeekStart:
		// no-op
	case io.SeekCurrent:
		offset = o + offset
	case io.SeekEnd:
		var err error
		offset, err = w.writer.Seek(offset, whence)
		if err != nil {
			return offset, err
		}
	}
	if offset != o {
		err := w.Flush()
		w.offset = offset
		if err != nil {
			return offset, err
		}
	}
	return offset, nil
}

type counting struct {
	count int64
}

type CountingReader struct {
	io.Reader
	counting
}

type CountingWriteCloser struct {
	io.WriteCloser
	counting
}

func (p *hashPool) get() (h hash.Hash) {
	l := len(*p)
	if l > 0 {
		l--
		h = (*p)[l]
		(*p)[l] = nil
		*p = (*p)[:l]
		h.Reset()
	} else {
		h, _ = blake2b.New512(nil)
	}
	return
}

func (p *hashPool) put(h hash.Hash) {
	*p = append(*p, h)
}

func (c *counting) Count() int64 {
	return c.count
}

func (r *CountingReader) Read(buf []byte) (n int, err error) {
	n, err = r.Reader.Read(buf)
	r.count += int64(n)
	return
}

func (r *CountingWriteCloser) Write(buf []byte) (n int, err error) {
	n, err = r.WriteCloser.Write(buf)
	r.count += int64(n)
	return
}

func (n *node) next() *node {
	if n.parent != nil {
		if n.idx < len(n.parent.children)-1 {
			return n.parent.children[n.idx+1]
		}
		nn := n.parent.next()
		if nn != nil {
			return nn.children[0]
		}
	}
	return nil
}

func (n *node) childReady(child *node, pool *hashPool, h hash.Hash) {
	if n.hash == nil {
		if h != nil {
			h.Reset()
			n.hash = h
		} else {
			n.hash = pool.get()
		}
	} else {
		if h != nil {
			pool.put(h)
		}
	}
	n.hash.Write(child.sum)
	if child.idx == len(n.children)-1 {
		n.sum = n.hash.Sum(n.buf[:0])
		if n.parent != nil {
			n.parent.childReady(n, pool, n.hash)
		}
		n.hash = nil
	}
}

func (b *base) buffer(size int64) []byte {
	if int64(cap(b.buf)) < size {
		b.buf = make([]byte, size+1)
	}
	return b.buf[:size]
}

func (t *tree) build(offset, length int64, order, level int) *node {
	n := &node{}
	level--
	if level > 0 {
		n.children = make([]*node, order)
		b := offset
		for i := 0; i < order; i++ {
			l := offset + (length * int64(i+1) / int64(order)) - b
			child := t.build(b, l, order, level)
			child.parent = n
			child.idx = i
			n.children[i] = child
			b += l
		}
	} else {
		n.size = int(length)
	}
	return n
}

func (t *tree) first(n *node) *node {
	if len(n.children) > 0 {
		return t.first(n.children[0])
	}
	return n
}

func (t *tree) calc(verbose bool) error {

	var targetBlockSize int64 = DefTargetBlockSize

	for t.size/targetBlockSize > 1048576 {
		targetBlockSize <<= 1
	}

	blocks := t.size / targetBlockSize

	levels := 8
	order := 1

	if blocks > 0 {
		var d int64 = -1
		for {
			b := int64(math.Pow(float64(order+1), 7))
			bs := t.size / b
			if bs < targetBlockSize/2 {
				break
			}
			nd := targetBlockSize - bs
			if nd < 0 {
				nd = -nd
			}
			// log.Printf("b: %d, d: %d\n", b, nd)
			if d != -1 && nd > d {
				break
			}
			d = nd
			order++
		}
		if order < 2 {
			order = 2
			levels = int(math.Log2(float64(blocks))) + 1
		}
	} else {
		levels = 1
		order = 1
	}

	bs := int(float64(t.size) / math.Pow(float64(order), float64(levels-1)))

	if verbose {
		log.Printf("Levels: %d, order: %d, target block size: %d, block size: %d\n", levels, order, targetBlockSize, bs)
	}

	t.root = t.build(0, t.size, order, levels)

	rr := int64(0)

	var reader io.Reader

	if t.useBuffer {
		var bufSize int
		for bufSize = DefTargetBlockSize; bufSize < bs; bufSize <<= 1 {
		}

		reader = bufio.NewReaderSize(t.reader, bufSize)
	} else {
		reader = t.reader
	}

	var pool hashPool = make([]hash.Hash, 0, levels)

	workItems := make([]*workCtx, 2)
	for i := range workItems {
		workItems[i] = &workCtx{
			buf:       make([]byte, bs+1),
			avail:     make(chan struct{}, 1),
			hashReady: make(chan struct{}, 1),
		}
		workItems[i].hash, _ = blake2b.New512(nil)
		workItems[i].avail <- struct{}{}
	}

	go func() {
		idx := 0
		for {
			wi := workItems[idx]
			<-wi.hashReady
			if wi.n == nil {
				break
			}
			if wi.n.parent != nil {
				wi.n.parent.childReady(wi.n, &pool, nil)
			}
			wi.avail <- struct{}{}
			idx++
			if idx >= len(workItems) {
				idx = 0
			}
		}
	}()

	workIdx := 0

	for n := t.first(t.root); n != nil; n = n.next() {
		if n.size == 0 {
			panic("Leaf node size is zero")
		}

		wi := workItems[workIdx]

		<-wi.avail

		b := wi.buf[:n.size]
		r, err := io.ReadFull(reader, b)
		if err != nil {
			return fmt.Errorf("in calc at %d (expected %d, read %d): %w", rr, len(b), r, err)
		}
		rr += int64(r)

		wi.n = n

		go func() {
			wi.hash.Write(b)
			wi.n.sum = wi.hash.Sum(wi.n.buf[:0])
			wi.hash.Reset()
			wi.hashReady <- struct{}{}
		}()

		workIdx++
		if workIdx >= len(workItems) {
			workIdx = 0
		}

	}

	// wait until fully processed
	for i := range workItems {
		<-workItems[i].avail
	}

	// finish the goroutine
	workItems[workIdx].n = nil
	workItems[workIdx].hashReady <- struct{}{}

	if rr < t.size {
		return fmt.Errorf("read less data (%d) than expected (%d)", rr, t.size)
	}

	return nil
}

func readHeader(reader io.Reader) (size int64, err error) {
	buf := make([]byte, len(hdrMagic)+8)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return
	}

	if string(buf[:len(hdrMagic)]) != hdrMagic {
		err = ErrInvalidFormat
		return
	}

	size = int64(binary.LittleEndian.Uint64(buf[len(hdrMagic):]))
	return
}

func writeHeader(writer io.Writer, size int64) (err error) {
	buf := make([]byte, len(hdrMagic)+8)
	copy(buf, hdrMagic)
	binary.LittleEndian.PutUint64(buf[len(hdrMagic):], uint64(size))
	_, err = writer.Write(buf)
	return
}

func Source(reader io.ReadSeeker, size int64, cmdReader io.Reader, cmdWriter io.Writer, useBuffer bool, verbose bool) (err error) {
	err = writeHeader(cmdWriter, size)
	if err != nil {
		return
	}
	var remoteSize int64
	remoteSize, err = readHeader(cmdReader)
	if err != nil {
		return fmt.Errorf("could not read header: %w", err)
	}

	var commonSize int64

	if remoteSize < size {
		commonSize = remoteSize
	} else {
		commonSize = size
	}

	if commonSize > 0 {
		s := source{
			base: base{
				t: tree{
					reader:    reader,
					size:      commonSize,
					useBuffer: useBuffer,
				},
				cmdReader: cmdReader,
				cmdWriter: cmdWriter,
			},
			reader: reader,
		}

		err = s.t.calc(verbose)
		if err != nil {
			return
		}

		err = s.subtree(s.t.root, 0, commonSize)
		if err != nil {
			return
		}
	}

	if size > commonSize {
		// Write the tail
		if verbose {
			log.Print("Writing tail...")
		}
		_, err = reader.Seek(commonSize, io.SeekStart)
		if err != nil {
			return
		}

		holeStart := int64(-1)
		curPos := commonSize
		buf := make([]byte, DefTargetBlockSize)
		bw := bufio.NewWriterSize(cmdWriter, DefTargetBlockSize*2)

		for {
			var r int
			var stop bool
			r, err = io.ReadFull(reader, buf)
			if err != nil {
				if err == io.EOF {
					break
				}
				if err != io.ErrUnexpectedEOF {
					return fmt.Errorf("source, reading tail: %w", err)
				}
				buf = buf[:r]
				stop = true
				err = nil
			}
			if spgz.IsBlockZero(buf) {
				if holeStart == -1 {
					holeStart = curPos
				}
			} else {
				if holeStart != -1 {
					err = bw.WriteByte(cmdHole)
					if err != nil {
						return
					}

					err = binary.Write(bw, binary.LittleEndian, curPos-holeStart)
					if err != nil {
						return
					}

					holeStart = -1
				}
				err = bw.WriteByte(cmdBlock)
				if err != nil {
					return
				}
				_, err = bw.Write(buf)
				if err != nil {
					return
				}

			}
			if err != nil {
				return
			}
			curPos += int64(r)
			if stop {
				break
			}
		}
		if holeStart != -1 {
			err = bw.WriteByte(cmdHole)
			if err != nil {
				return
			}

			err = binary.Write(bw, binary.LittleEndian, curPos-holeStart)
			if err != nil {
				return
			}
		}
		err = bw.Flush()
	}

	return
}

func (s *source) subtree(root *node, offset, size int64) (err error) {
	remoteHash := make([]byte, hashSize)

	_, err = io.ReadFull(s.cmdReader, remoteHash)
	if err != nil {
		return fmt.Errorf("source/subtree, reading hash: %w", err)
	}

	if bytes.Equal(root.sum, remoteHash) {
		err = binary.Write(s.cmdWriter, binary.LittleEndian, cmdEqual)
		return
	}

	if root.size > 0 {
		// log.Printf("Blocks at %d don't match\n", offset)

		if int64(root.size) != size {
			panic("Leaf node size mismatch")
		}

		_, err = s.reader.Seek(offset, io.SeekStart)
		if err != nil {
			return
		}

		buf := s.buffer(size)
		_, err = io.ReadFull(s.reader, buf)
		if err != nil {
			return fmt.Errorf("source read failed at %d: %w", offset, err)
		}

		if spgz.IsBlockZero(buf) {
			err = binary.Write(s.cmdWriter, binary.LittleEndian, cmdHole)
		} else {
			err = binary.Write(s.cmdWriter, binary.LittleEndian, cmdNotEqual)
			if err != nil {
				return
			}

			_, err = s.cmdWriter.Write(buf)
		}
	} else {
		err = binary.Write(s.cmdWriter, binary.LittleEndian, cmdNotEqual)
		if err != nil {
			return
		}

		b := offset
		order := byte(len(root.children))
		for i := byte(0); i < order; i++ {
			l := offset + (size * int64(i+1) / int64(order)) - b
			err = s.subtree(root.children[i], b, l)
			if err != nil {
				return
			}
			b += l
		}
	}

	return
}

type TargetFile interface {
	io.ReadWriteSeeker
	io.WriterAt
	io.Closer
	spgz.Truncatable
}

// FixingSpgzFileWrapper conceals read errors caused by compressed data corruption by re-writing the corrupt
// blocks with zeros. Such errors are usually caused by abrupt termination of the writing process.
// This wrapper is used as the sync target so the corrupt blocks will be updated during the sync process.
type FixingSpgzFileWrapper struct {
	*spgz.SpgzFile
}

func (rw *FixingSpgzFileWrapper) checkErr(err error) error {
	var ce *spgz.ErrCorruptCompressedBlock
	if errors.As(err, &ce) {
		if ce.Size() == 0 {
			return rw.SpgzFile.Truncate(ce.Offset())
		}

		buf := make([]byte, ce.Size())
		_, err = rw.SpgzFile.WriteAt(buf, ce.Offset())
	}
	return err
}

func (rw *FixingSpgzFileWrapper) Read(p []byte) (n int, err error) {
	for n == 0 && err == nil { // avoid returning (0, nil) after a fix
		n, err = rw.SpgzFile.Read(p)
		if err != nil {
			err = rw.checkErr(err)
		}
	}
	return
}

func (rw *FixingSpgzFileWrapper) Seek(offset int64, whence int) (int64, error) {
	o, err := rw.SpgzFile.Seek(offset, whence)
	if err != nil {
		err = rw.checkErr(err)
		if err == nil {
			o, err = rw.SpgzFile.Seek(offset, whence)
		}
	}
	return o, err
}

func Target(writer spgz.SparseFile, size int64, cmdReader io.Reader, cmdWriter io.Writer, useReadBuffer bool, verbose bool) (err error) {

	ch := make(chan error)
	go func() {
		ch <- writeHeader(cmdWriter, size)
	}()

	var remoteSize int64
	remoteSize, err = readHeader(cmdReader)
	if err != nil {
		return
	}

	err = <-ch
	if err != nil {
		return
	}

	commonSize := size
	if remoteSize < commonSize {
		commonSize = remoteSize
	}

	if verbose {
		log.Printf("Local size: %d, remote size: %d", size, remoteSize)
	}

	if commonSize > 0 {
		t := target{
			base: base{
				t: tree{
					reader:    writer,
					size:      commonSize,
					useBuffer: useReadBuffer,
				},
				cmdReader: cmdReader,
				cmdWriter: cmdWriter,
			},
			writer: &batchingWriter{writer: writer, maxSize: DefTargetBlockSize * 16},
		}
		err = t.t.calc(verbose)
		if err != nil {
			return
		}

		err = t.subtree(t.t.root, 0, commonSize)
		if err != nil {
			return
		}
		err = t.writer.Flush()
		if err != nil {
			return
		}
	}

	if size < remoteSize {
		// Read the tail
		if verbose {
			log.Printf("Reading tail (%d bytes)...", remoteSize-size)
		}
		_, err = writer.Seek(commonSize, io.SeekStart)
		if err != nil {
			return
		}

		hole := false
		rd := bufio.NewReaderSize(cmdReader, DefTargetBlockSize*2)

		for {
			var cmd byte
			cmd, err = rd.ReadByte()
			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				return fmt.Errorf("target: while reading tail block header: %w", err)
			}

			if cmd == cmdBlock {
				_, err = io.CopyN(writer, rd, DefTargetBlockSize)

				hole = false
				if err != nil {
					if err == io.EOF {
						err = nil
						break
					} else {
						return fmt.Errorf("target: while copying block: %w", err)
					}
				}
			} else {
				if cmd == cmdHole {
					var holeSize int64
					err = binary.Read(rd, binary.LittleEndian, &holeSize)
					if err != nil {
						return fmt.Errorf("target: while reading hole size: %w", err)
					}
					_, err = writer.Seek(holeSize, io.SeekCurrent)
					if err != nil {
						return
					}
					hole = true
				} else {
					return fmt.Errorf("unexpected cmd: %d", cmd)
				}
			}
		}

		if hole {
			if f, ok := writer.(spgz.Truncatable); ok {
				err = f.Truncate(remoteSize)
			}
		}

	} else if size > remoteSize {
		// Truncate target
		if f, ok := writer.(spgz.Truncatable); ok {
			err = f.Truncate(commonSize)
		}
	}

	return
}

func (t *target) subtree(root *node, offset, size int64) (err error) {
	_, err = t.cmdWriter.Write(root.sum)
	if err != nil {
		return
	}

	var cmd byte
	err = binary.Read(t.cmdReader, binary.LittleEndian, &cmd)
	if err != nil {
		return fmt.Errorf("target: while reading block header at %d: %w", offset, err)
	}

	// log.Printf("offset: %d, size: %d, cmd: %d\n", offset, size, cmd)

	if cmd == cmdNotEqual || cmd == cmdHole {
		if root.size > 0 {
			_, err = t.writer.Seek(offset, io.SeekStart)
			if err != nil {
				return
			}

			if cmd == cmdNotEqual {
				_, err = io.CopyN(t.writer, t.cmdReader, size)
				if err != nil {
					err = fmt.Errorf("while copying block data at %d: %w", offset, err)
				}
			} else {
				err = t.writer.WriteHole(size)
			}
		} else {
			b := offset
			order := byte(len(root.children))
			for i := byte(0); i < order; i++ {
				l := offset + (size * int64(i+1) / int64(order)) - b
				err = t.subtree(root.children[i], b, l)
				if err != nil {
					return
				}
				b += l
			}
		}
	}

	return
}
