package diskrsync

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"github.com/codahale/blake2"
	"hash"
	"io"
	"math/rand"
	"os"
	"testing"
)

type memFile struct {
	data   []byte
	offset int64
}

func (s *memFile) Read(buf []byte) (n int, err error) {
	if s.offset >= int64(len(s.data)) {
		err = io.EOF
		return
	}
	n = copy(buf, s.data[s.offset:])
	s.offset += int64(n)
	return
}

func (s *memFile) Write(buf []byte) (n int, err error) {
	newSize := s.offset + int64(len(buf))
	if newSize > int64(len(s.data)) {
		if newSize <= int64(cap(s.data)) {
			s.data = s.data[:newSize]
		} else {
			d := make([]byte, newSize)
			copy(d, s.data)
			s.data = d
		}
	}
	n = copy(s.data[s.offset:], buf)
	if n < len(buf) {
		err = io.ErrShortWrite
	}
	s.offset += int64(n)
	return
}

func (s *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case os.SEEK_SET:
		s.offset = offset
		return s.offset, nil
	case os.SEEK_CUR:
		s.offset += offset
		return s.offset, nil
	case os.SEEK_END:
		s.offset = int64(len(s.data)) + offset
		return s.offset, nil
	}
	return s.offset, errors.New("Invalid whence")
}

func (s *memFile) Truncate(size int64) error {
	if size > int64(len(s.data)) {
		if size <= int64(cap(s.data)) {
			l := len(s.data)
			s.data = s.data[:size]
			for i := l; i < len(s.data); i++ {
				s.data[i] = 0
			}
		} else {
			d := make([]byte, size)
			copy(d, s.data)
			s.data = d
		}
	} else if size < int64(len(s.data)) {
		s.data = s.data[:size]
	}
	return nil
}

func (s *memFile) Bytes() []byte {
	return s.data
}

func TestZero(t *testing.T) {
	src := make([]byte, 1*1024*1024)
	dst := make([]byte, 1*1024*1024)
	for i := 0; i < len(src); i++ {
		src[i] = byte(i)
	}

	copy(dst, src)

	for i := 33; i < 133000; i++ {
		src[i] = 0
	}

	syncAndCheckEqual(src, dst, t)
}

func TestRandomChange(t *testing.T) {
	src := make([]byte, 2*1024*1024)
	dst := make([]byte, 2*1024*1024)

	for i := 0; i < len(src); i++ {
		src[i] = byte(rand.Int31n(256))
	}

	copy(dst, src)

	for i := 333; i < 133000; i++ {
		src[i] = byte(rand.Int31n(256))
	}

	syncAndCheckEqual(src, dst, t)
}

func TestExpand(t *testing.T) {
	dst := make([]byte, 2*1024*1024)

	for i := 0; i < len(dst); i++ {
		dst[i] = byte(rand.Int31n(256))
	}

	src := make([]byte, 2*1024*1024+333333)

	copy(src, dst)

	for i := len(dst); i < len(src); i++ {
		src[i] = byte(rand.Int31n(256))
	}

	syncAndCheckEqual(src, dst, t)
}

func TestShrink(t *testing.T) {
	dst := make([]byte, 2*1024*1024+333333)

	for i := 0; i < len(dst); i++ {
		dst[i] = byte(rand.Int31n(256))
	}

	src := make([]byte, 2*1024*1024)

	copy(src, dst)

	syncAndCheckEqual(src, dst, t)
}

func TestNoChange(t *testing.T) {
	src := make([]byte, 2*1024*1024)
	dst := make([]byte, 2*1024*1024)

	for i := 0; i < len(src); i++ {
		src[i] = byte(rand.Int31n(256))
	}

	copy(dst, src)

	sent, received := syncAndCheckEqual(src, dst, t)
	if sent != 17 {
		t.Fatalf("Sent %d bytes (expected 17)", sent)
	}
	if received != 80 {
		t.Fatalf("Received %d bytes (expected 80)", received)
	}
}

func syncAndCheckEqual(src, dst []byte, t *testing.T) (sent, received int64) {
	srcReader, dstWriter := io.Pipe()
	dstReader, srcWriter := io.Pipe()

	srcR := &memFile{data: src}
	dstW := &memFile{data: dst}

	dstReaderC := &CountingReader{Reader: dstReader}
	dstWriterC := &CountingWriteCloser{WriteCloser: dstWriter}

	go func() {
		err := Source(srcR, int64(len(src)), srcReader, srcWriter, false, false)
		if err != nil {
			t.Fatal(err)
		}
		srcWriter.Close()
	}()

	err := Target(dstW, int64(len(dst)), dstReaderC, dstWriterC, false, false)
	dstWriter.Close()

	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(srcR.Bytes(), dstW.Bytes()) {
		t.Fatal("Not equal")
	}

	return dstReaderC.Count(), dstWriterC.Count()
}

func BenchmarkBlake2(b *testing.B) {
	b.StopTimer()
	h := blake2.NewBlake2B()
	buf := make([]byte, 4096)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		h.Write(buf)
	}
}

func BenchmarkBlake256(b *testing.B) {
	b.StopTimer()
	conf := blake2.Config{
		Size: 32,
	}
	h := blake2.New(&conf)
	buf := make([]byte, 4096)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		h.Write(buf)
	}

}

func BenchmarkSHA1(b *testing.B) {
	b.StopTimer()
	h := sha1.New()
	buf := make([]byte, 4096)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		h.Write(buf)
	}

}

func BenchmarkSequential(b *testing.B) {
	b.StopTimer()
	hashes := make([]hash.Hash, 8)
	for i := 0; i < len(hashes); i++ {
		hashes[i] = blake2.NewBlake2B()
	}
	buf := make([]byte, 32768)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(hashes); j++ {
			hashes[j].Write(buf)
		}
	}
}

func BenchmarkParallel(b *testing.B) {
	b.StopTimer()
	hashes := make([]hash.Hash, 8)
	for i := 0; i < len(hashes); i++ {
		hashes[i] = blake2.NewBlake2B()
	}
	buf := make([]byte, 32768)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	ch := make(chan int, 8)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(hashes); j++ {
			go func(idx int) {
				hashes[idx].Write(buf)
				ch <- 1
			}(j)
		}

		for j := 0; j < len(hashes); j++ {
			<-ch
		}

	}
}
