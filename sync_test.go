package diskrsync

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"hash"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/dop251/spgz"

	"golang.org/x/crypto/blake2b"
)

type memSparseFile struct {
	data   []byte
	offset int64
}

func (s *memSparseFile) Read(buf []byte) (n int, err error) {
	if s.offset >= int64(len(s.data)) {
		err = io.EOF
		return
	}
	n = copy(buf, s.data[s.offset:])
	s.offset += int64(n)
	return
}

func (s *memSparseFile) ensureSize(newSize int64) {
	if newSize > int64(len(s.data)) {
		if newSize <= int64(cap(s.data)) {
			l := int64(len(s.data))
			s.data = s.data[:newSize]
			for i := l; i < s.offset; i++ {
				s.data[i] = 0
			}
		} else {
			d := make([]byte, newSize)
			copy(d, s.data)
			s.data = d
		}
	}
}

func (s *memSparseFile) Write(buf []byte) (n int, err error) {
	s.ensureSize(s.offset + int64(len(buf)))
	n = copy(s.data[s.offset:], buf)
	if n < len(buf) {
		err = io.ErrShortWrite
	}
	s.offset += int64(n)
	return
}

func (s *memSparseFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		s.offset = offset
		return s.offset, nil
	case io.SeekCurrent:
		s.offset += offset
		return s.offset, nil
	case io.SeekEnd:
		s.offset = int64(len(s.data)) + offset
		return s.offset, nil
	}
	return s.offset, errors.New("invalid whence")
}

func (s *memSparseFile) Truncate(size int64) error {
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

func (s *memSparseFile) PunchHole(offset, size int64) error {
	if offset < int64(len(s.data)) {
		d := offset + size - int64(len(s.data))
		if d > 0 {
			size -= d
		}
		for i := offset; i < offset+size; i++ {
			s.data[i] = 0
		}
	}
	return nil
}

func (s *memSparseFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off < int64(len(s.data)) {
		n = copy(p, s.data[off:])
	}
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (s *memSparseFile) WriteAt(p []byte, off int64) (n int, err error) {
	s.ensureSize(off + int64(len(p)))
	n = copy(s.data[off:], p)
	return
}

func (s *memSparseFile) Close() error {
	return nil
}

func (s *memSparseFile) Sync() error {
	return nil
}

func (s *memSparseFile) Bytes() []byte {
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

func TestExpandWithZeros(t *testing.T) {
	dst := make([]byte, 2*1024*1024)

	for i := 0; i < len(dst); i++ {
		dst[i] = byte(rand.Int31n(256))
	}

	src := make([]byte, 2*1024*1024+333333)

	copy(src, dst)

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

func TestSmallFile(t *testing.T) {
	src := make([]byte, 128)
	dst := make([]byte, 128)

	for i := range src {
		src[i] = 'x'
	}

	syncAndCheckEqual(src, dst, t)
}

func TestCorruptCompressedBlock(t *testing.T) {
	var f memSparseFile
	sf, err := spgz.NewFromSparseFileSize(&f, os.O_RDWR|os.O_CREATE, 3*4096)
	if err != nil {
		t.Fatal(err)
	}
	src := make([]byte, 2*1024*1024)

	for i := 0; i < len(src); i++ {
		src[i] = 'x'
	}

	_, err = sf.WriteAt(src, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = sf.Close()
	if err != nil {
		t.Fatal(err)
	}

	if f.data[4096] != 1 {
		t.Fatalf("data: %d", f.data[4096])
	}

	f.data[4098] = ^f.data[4098]

	_, _ = f.Seek(0, io.SeekStart)

	sf, err = spgz.NewFromSparseFileSize(&f, os.O_RDWR, 4096)
	if err != nil {
		t.Fatal(err)
	}

	syncAndCheckEqual1(&memSparseFile{data: src}, &FixingSpgzFileWrapper{SpgzFile: sf}, t)
}

func TestCorruptLastCompressedBlock(t *testing.T) {
	var f memSparseFile
	sf, err := spgz.NewFromSparseFileSize(&f, os.O_RDWR|os.O_CREATE, 3*4096)
	if err != nil {
		t.Fatal(err)
	}
	src := make([]byte, 2*1024*1024)

	for i := 0; i < len(src); i++ {
		src[i] = 'x'
	}

	_, err = sf.WriteAt(src, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = sf.Close()
	if err != nil {
		t.Fatal(err)
	}

	offset := 4096 + len(src)/(3*4096-1)*(3*4096)

	if f.data[offset] != 1 {
		t.Fatalf("data: %d", f.data[offset])
	}

	f.data[offset+2] = ^f.data[offset+2]

	_, _ = f.Seek(0, io.SeekStart)

	sf, err = spgz.NewFromSparseFileSize(&f, os.O_RDWR, 4096)
	if err != nil {
		t.Fatal(err)
	}

	syncAndCheckEqual1(&memSparseFile{data: src}, &FixingSpgzFileWrapper{SpgzFile: sf}, t)
}

func TestRandomFiles(t *testing.T) {
	var srcFile, dstFile memSparseFile
	sf, err := spgz.NewFromSparseFile(&dstFile, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}
	rand.Seed(1234567890)
	buf := make([]byte, 100*DefTargetBlockSize)
	rand.Read(buf)
	_, err = sf.WriteAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}

	o, err := sf.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if o != 0 {
		t.Fatalf("o: %d", o)
	}

	rand.Read(buf)
	_, err = srcFile.WriteAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}

	syncAndCheckEqual1(&srcFile, sf, t)
}

type testOplogItem struct {
	offset int64
	length int
}

type testLoggingSparseFile struct {
	spgz.SparseFile
	wrlog []testOplogItem
}

func (f *testLoggingSparseFile) WriteAt(buf []byte, offset int64) (int, error) {
	f.wrlog = append(f.wrlog, testOplogItem{
		offset: offset,
		length: len(buf),
	})
	return f.SparseFile.WriteAt(buf, offset)
}

func TestBatchingWriter(t *testing.T) {
	var sf memSparseFile
	lsf := &testLoggingSparseFile{
		SparseFile: &sf,
	}
	wr := &batchingWriter{
		writer:  lsf,
		maxSize: 100,
	}

	reset := func(t *testing.T) {
		_, err := wr.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		lsf.wrlog = lsf.wrlog[:0]
		sf.data = nil
		sf.offset = 0
	}

	t.Run("large_chunk", func(t *testing.T) {
		buf := make([]byte, 502)
		rand.Read(buf)
		reset(t)
		n, err := wr.Write(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(buf) {
			t.Fatal(n)
		}
		err = wr.Flush()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(sf.Bytes(), buf) {
			t.Fatal("not equal")
		}
		if !reflect.DeepEqual(lsf.wrlog, []testOplogItem{
			{offset: 0, length: 500},
			{offset: 500, length: 2},
		}) {
			t.Fatalf("Oplog: %#v", lsf.wrlog)
		}
	})

	t.Run("exact", func(t *testing.T) {
		buf := make([]byte, 100)
		rand.Read(buf)
		reset(t)
		n, err := wr.Write(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(buf) {
			t.Fatal(n)
		}
		err = wr.Flush()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(sf.Bytes(), buf) {
			t.Fatal("not equal")
		}
		if !reflect.DeepEqual(lsf.wrlog, []testOplogItem{
			{offset: 0, length: 100},
		}) {
			t.Fatalf("Oplog: %#v", lsf.wrlog)
		}
	})

	t.Run("two_small", func(t *testing.T) {
		buf := make([]byte, 100)
		rand.Read(buf)
		reset(t)
		n, err := wr.Write(buf[:50])
		if err != nil {
			t.Fatal(err)
		}
		if n != 50 {
			t.Fatal(n)
		}

		n, err = wr.Write(buf[50:])
		if err != nil {
			t.Fatal(err)
		}
		if n != 50 {
			t.Fatal(n)
		}

		err = wr.Flush()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(sf.Bytes(), buf) {
			t.Fatal("not equal")
		}
		if !reflect.DeepEqual(lsf.wrlog, []testOplogItem{
			{offset: 0, length: 100},
		}) {
			t.Fatalf("Oplog: %#v", lsf.wrlog)
		}
	})

	t.Run("seek", func(t *testing.T) {
		buf := make([]byte, 100)
		rand.Read(buf)
		reset(t)
		n, err := wr.Write(buf[:50])
		if err != nil {
			t.Fatal(err)
		}
		if n != 50 {
			t.Fatal(n)
		}

		o, err := wr.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if o != 50 {
			t.Fatal(o)
		}

		o, err = wr.Seek(55, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if o != 55 {
			t.Fatal(o)
		}

		n, err = wr.Write(buf[50:])
		if err != nil {
			t.Fatal(err)
		}
		if n != 50 {
			t.Fatal(n)
		}
		err = wr.Flush()
		if err != nil {
			t.Fatal(err)
		}

		exp := make([]byte, 105)
		copy(exp, buf[:50])
		copy(exp[55:], buf[50:])

		if !bytes.Equal(sf.Bytes(), exp) {
			t.Fatal("not equal")
		}
		if !reflect.DeepEqual(lsf.wrlog, []testOplogItem{
			{offset: 0, length: 50},
			{offset: 55, length: 50},
		}) {
			t.Fatalf("Oplog: %#v", lsf.wrlog)
		}
	})
}

func TestFuzz(t *testing.T) {
	const (
		fileSize = 30 * 1024 * 1024

		numBlocks      = 50
		numBlocksDelta = 128

		blockSize      = 64 * 1024
		blockSizeDelta = 32 * 1024
	)

	if testing.Short() {
		t.Skip()
	}
	seed := time.Now().UnixNano()
	t.Logf("Seed: %d", seed)
	rnd := rand.New(rand.NewSource(seed))
	roll := func(mean, delta int) int {
		return mean + int(rnd.Int31n(int32(delta))) - delta/2
	}

	srcBuf := make([]byte, fileSize)
	dstBuf := make([]byte, fileSize)

	blockBuf := make([]byte, 0, blockSize+blockSizeDelta/2)
	zeroBlockBuf := make([]byte, 0, blockSize+blockSizeDelta/2)

	mutateBlock := func(buf []byte) {
		size := roll(blockSize, blockSizeDelta)
		offset := int(rnd.Int31n(int32(len(buf))))
		blk := blockBuf[:size]
		typ := rnd.Int31n(16)
		if typ >= 5 {
			rnd.Read(blk)
		} else if typ >= 3 {
			for i := range blk {
				blk[i] = 'x'
			}
		} else {
			blk = zeroBlockBuf[:size]
		}
		copy(buf[offset:], blk)
	}

	for i := 0; i < 50; i++ {
		t.Logf("Running file %d", i)
		dice := rnd.Int31n(16)
		var srcSize, dstSize int
		srcSize = int(rnd.Int31n(fileSize))
		if dice > 4 {
			dstSize = int(rnd.Int31n(fileSize))
		} else {
			dstSize = srcSize
		}
		srcBuf = srcBuf[:srcSize]
		dstBuf = dstBuf[:dstSize]
		rnd.Read(srcBuf)
		nBlocks := roll(numBlocks, numBlocksDelta)
		for i := 0; i < nBlocks; i++ {
			mutateBlock(srcBuf)
		}

		copy(dstBuf, srcBuf)

		nBlocks = roll(numBlocks, numBlocksDelta)
		for i := 0; i < nBlocks; i++ {
			mutateBlock(dstBuf)
		}

		var mf memSparseFile
		sf, err := spgz.NewFromSparseFile(&mf, os.O_RDWR|os.O_CREATE)
		if err != nil {
			t.Fatal(err)
		}
		_, err = sf.WriteAt(dstBuf, 0)
		if err != nil {
			t.Fatal(err)
		}
		sent, received := syncAndCheckEqual(srcBuf, dstBuf, t)
		t.Logf("src size: %d, sent: %d, received: %d", len(srcBuf), sent, received)
		sent1, received1 := syncAndCheckEqual1(&memSparseFile{data: srcBuf}, sf, t)
		if sent != sent1 {
			t.Fatalf("Sent counts did not match: %d, %d", sent, sent1)
		}
		if received != received1 {
			t.Fatalf("Received counts did not match: %d, %d", received, received1)
		}
	}
}

func syncAndCheckEqual(src, dst []byte, t *testing.T) (sent, received int64) {
	return syncAndCheckEqual1(&memSparseFile{data: src}, &memSparseFile{data: dst}, t)
}

func getSize(s io.Seeker) (int64, error) {
	o, err := s.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	size, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	_, err = s.Seek(o, io.SeekStart)
	return size, err
}

func getBytes(r io.ReadSeeker) ([]byte, error) {
	if b, ok := r.(interface {
		Bytes() []byte
	}); ok {
		return b.Bytes(), nil
	}

	_, err := r.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(r)
}

func syncAndCheckEqual1(src io.ReadSeeker, dst spgz.SparseFile, t *testing.T) (sent, received int64) {
	srcReader, dstWriter := io.Pipe()
	dstReader, srcWriter := io.Pipe()

	dstReaderC := &CountingReader{Reader: dstReader}
	dstWriterC := &CountingWriteCloser{WriteCloser: dstWriter}

	srcErrChan := make(chan error, 1)

	srcSize, err := getSize(src)
	if err != nil {
		t.Fatal(err)
	}
	dstSize, err := getSize(dst)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		err := Source(src, srcSize, srcReader, srcWriter, false, false, nil, nil)
		cerr := srcWriter.Close()
		if err == nil {
			err = cerr
		}
		srcErrChan <- err
	}()

	err = Target(dst, dstSize, dstReaderC, dstWriterC, false, false, nil, nil)
	cerr := dstWriter.Close()
	if err == nil {
		err = cerr
	}

	if err != nil {
		t.Fatal(err)
	}

	if err = <-srcErrChan; err != nil {
		t.Fatal(err)
	}

	srcBytes, err := getBytes(src)
	if err != nil {
		t.Fatal(err)
	}

	dstBytes, err := getBytes(dst)
	if err != nil {
		t.Fatal(err)
	}

	if len(srcBytes) != len(dstBytes) {
		t.Fatalf("Len not equal: %d, %d", len(srcBytes), len(dstBytes))
	}
	for i := 0; i < len(srcBytes); i++ {
		if srcBytes[i] != dstBytes[i] {
			t.Fatalf("Data mismatch at %d: %d, %d", i, srcBytes[i], dstBytes[i])
		}
	}
	/*if !bytes.Equal(srcBytes, dstBytes) {
		t.Fatal("Not equal")
	}*/

	return dstReaderC.Count(), dstWriterC.Count()
}

func BenchmarkBlake2(b *testing.B) {
	b.StopTimer()
	h, _ := blake2b.New512(nil)
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
	h, _ := blake2b.New256(nil)
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
		hashes[i], _ = blake2b.New512(nil)
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
		hashes[i], _ = blake2b.New512(nil)
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
