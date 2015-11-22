//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"
)

const c_CHUNKSIZE = 2048              // 2KiB chunks
const c_SLABSIZE = c_CHUNKSIZE * 2048 // 4MiB slabs
const c_MAXSLABS = 4096               // 16GiB memory

const (
	SEEK_SET int = 0 // seek relative to the origin of the array
	SEEK_CUR int = 1 // seek relative to the current offset
	SEEK_END int = 2 // seek relative to the end of the array
)

type position struct {
	current  int
	chunkIX  int
	chunkPos int
	chunk    uint32
}

// Byte array read and write is not concurrency safe however the underlying slab structures
// are so you can use multiple ByteArrays at the same time
type ByteArray struct {
	rootChunk  uint32
	rootOffset int
	usedBytes  int
	writePos   position
	readPos    position
}

// WritePosition returns the current write position
func (b ByteArray) WritePosition() int {
	return b.writePos.current
}

// ReadPosition returns the current write position
func (b ByteArray) ReadPosition() int {
	return b.readPos.current
}

// Len returns the current length of the ByteArray
func (b ByteArray) Len() int {
	return b.usedBytes
}

// Release will release all chunks associated with the ByteArray
func (b *ByteArray) Release() {
	b.Truncate(0)
	if b.rootChunk != emptyLocation {
		releaseChunk(b.rootChunk)
		b.rootChunk = emptyLocation
	}
}

// Split a byte array into a new ByteArray at the specified offset
func (b *ByteArray) Split(offset int) (newArray ByteArray) {
	if offset > b.usedBytes {
		panic("ASSERT")
	}
	if b.rootChunk == emptyLocation {
		panic("ASSERT")
	}

	if offset == 0 {
		newArray.rootChunk = b.rootChunk
		b.rootChunk = emptyLocation
	} else if offset == b.usedBytes {
		// optimization because no copy or rootOffset is needed
	} else if (b.rootOffset+offset)%c_CHUNKSIZE > 0 { // Split inside a chunk
		var splitPosition position
		splitPosition = b.seek(splitPosition, offset, SEEK_SET)
		splitSlice := getSlice(splitPosition)

		newArray.rootOffset = splitPosition.chunkPos
		newArray.prepare()
		newSlice := newArray.WriteSlice()

		// Duplicate the split block
		copy(newSlice, splitSlice)
		setNextLocation(newArray.rootChunk, getNextLocation(splitPosition.chunk))
		setNextLocation(splitPosition.chunk, emptyLocation)
	} else {
		var splitPosition position
		splitPosition = b.seek(splitPosition, offset-1, SEEK_SET) // -1 just to get the index of the previous chunk
		newArray.rootChunk = getNextLocation(splitPosition.chunk)
		setNextLocation(splitPosition.chunk, emptyLocation)
	}
	newArray.usedBytes = b.usedBytes - offset
	newArray.writePos = newArray.seek(newArray.writePos, 0, SEEK_END)
	if newArray.readPos.current != 0 {
		panic("ASSERT!")
	}
	//newArray.readPos = newArray.seek(newArray.readPos, 0, SEEK_SET)
	b.Truncate(offset)
	return
}

// Truncate sets the length, it also expands the length in case offset > usedBytes
func (b *ByteArray) Truncate(offset int) int {
	var p position
	p = b.seek(p, offset, SEEK_SET)
	for next := getNextLocation(p.chunk); next != emptyLocation; {
		releaseMe := next
		next = getNextLocation(next)
		releaseChunk(releaseMe)
	}
	setNextLocation(p.chunk, emptyLocation)
	b.usedBytes = p.current
	if b.readPos.current > b.usedBytes {
		b.readPos = b.seek(b.readPos, 0, SEEK_END)
	}
	if b.writePos.current > b.usedBytes {
		b.writePos = b.seek(b.writePos, 0, SEEK_END)
	}
	return b.usedBytes
}

// seek does not limit on size, it will allocate and grow
func (b *ByteArray) seek(was position, offset int, whence int) (now position) {
	switch whence {
	case SEEK_SET:
		now.current = offset
	case SEEK_CUR:
		now.current = was.current + offset
	case SEEK_END:
		now.current = b.usedBytes - offset
	}
	now.chunkPos = (now.current + b.rootOffset) % c_CHUNKSIZE
	now.chunkIX = (now.current + b.rootOffset) / c_CHUNKSIZE
	if was.chunkIX == 0 || now.chunkIX < was.chunkIX { // Chunks are only linked forward, so in reverse we need to restart
		was.chunkIX = 0
		b.prepare()
		was.chunk = b.rootChunk
	}
	now.chunk = was.chunk
	for was.chunkIX < now.chunkIX {
		if now.chunk == emptyLocation {
			panic("ASSERT")
		}
		if getNextLocation(now.chunk) == emptyLocation {
			now.chunk = appendChunk(now.chunk)
		} else {
			now.chunk = getNextLocation(now.chunk)
		}
		was.chunkIX++
	}
	if now.current > b.usedBytes {
		b.usedBytes = now.current
	}
	return now
}

// prepare a ByteArray for writing
func (b *ByteArray) prepare() {
	if b.rootChunk == emptyLocation {
		b.rootChunk = grabChunk()
		b.readPos = b.seek(b.readPos, 0, SEEK_SET)
		b.writePos = b.seek(b.writePos, 0, SEEK_SET)
	}
}

// WriteSeek will allocate and expand bounds if needed
func (b *ByteArray) WriteSeek(offset int, whence int) int {
	b.writePos = b.seek(b.writePos, offset, whence)
	return b.writePos.current
}

// ReadSeek will check bounds and return EOF error if seeking outside
func (b *ByteArray) ReadSeek(offset int, whence int) (absolute int, err error) {
	switch whence {
	case SEEK_SET:
		absolute = offset
	case SEEK_CUR:
		absolute = b.readPos.current + offset
	case SEEK_END:
		absolute = b.usedBytes - offset
	}
	if absolute < 0 {
		absolute = 0
		err = io.EOF
	}
	if absolute > b.usedBytes {
		absolute = b.usedBytes
		err = io.EOF
	}
	b.readPos = b.seek(b.readPos, absolute, SEEK_SET)
	return b.readPos.current, err
}

// ReadSlice returns a byte slice chunk for the current read position (it does not advance read position)
func (b *ByteArray) ReadSlice() ([]byte, error) {
	if b.readPos.current >= b.usedBytes {
		return nil, io.EOF
	}
	slice := getSlice(b.readPos)
	if len(slice) > b.usedBytes-b.readPos.current {
		return slice[:b.usedBytes-b.readPos.current], nil
	} else {
		return slice, nil
	}
}

// Read from the byte array into a buffer and advance the current read position
func (b *ByteArray) Read(p []byte) (n int, err error) {
	for n = 0; n < len(p); {
		var slice []byte
		slice, err = b.ReadSlice()
		if slice != nil {
			read := copy(p[n:], slice)
			b.readPos = b.seek(b.readPos, read, SEEK_CUR)
			n += read
		} else {
			break
		}
	}
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (b *ByteArray) WriteTo(w io.Writer) (n int64, err error) {
	for b.readPos.current < b.usedBytes {
		slice, er := b.ReadSlice()
		if slice != nil {
			read, err := w.Write(slice)
			b.readPos = b.seek(b.readPos, read, SEEK_CUR)
			n += int64(read)
			if err != nil {
				return n, err
			}
		} else {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return n, err
}

// WriteSlice returns a byte slice chunk for the current write position (it does not advance write position)
func (b *ByteArray) WriteSlice() []byte {
	b.prepare()
	return getSlice(b.writePos)
}

// Write to the byte array from a buffer and advance the current write position
func (b *ByteArray) Write(p []byte) (n int, err error) {
	for n = 0; n < len(p); {
		var slice []byte
		slice = b.WriteSlice()
		if slice == nil {
			panic("ASSERT")
		}

		written := copy(slice, p[n:])
		b.writePos = b.seek(b.writePos, written, SEEK_CUR)
		n += written
	}
	return n, err
}

func (b *ByteArray) ReadFrom(r io.Reader) (n int64, err error) {
	for {
		slice := b.WriteSlice()
		if slice == nil {
			panic("ASSERT")
		}

		written, er := r.Read(slice)
		b.writePos = b.seek(b.writePos, written, SEEK_CUR)
		n += int64(written)
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return n, err
}

/****************/

var slabs [c_MAXSLABS]*byteSlab
var freeChunk uint32 = emptyLocation
var allocatedSlabs uint32
var grabbedChunks int64
var releasedChunks int64

const emptyLocation uint32 = 0 // Location is empty

var memoryMutex sync.Mutex

type byteChunkLocation uint32 // upper 16bit is slabIndex, lower 16bit is chunkIndex
func getChunkLocation(chunk uint32) (slabIndex, chunkIndex uint16) {
	return uint16(chunk >> 16), uint16(chunk)
}
func setChunkLocation(slabIndex, chunkIndex uint16) uint32 {
	return uint32(slabIndex)<<16 | uint32(chunkIndex)
}
func getNextLocation(chunk uint32) uint32 {
	if chunk == emptyLocation {
		return emptyLocation
	} else {
		return slabs[(chunk>>16)&0xffff].next[(chunk & 0xffff)]
	}
}
func setNextLocation(chunk uint32, next uint32) {
	if chunk == emptyLocation {
		panic("ASSERT!")
	}
	slabs[(chunk>>16)&0xffff].next[(chunk & 0xffff)] = next
}

// getSlice gets a byte slice for a chunk position
func getSlice(p position) []byte {
	s, i := getChunkLocation(p.chunk)
	bufStart := int(i)*c_CHUNKSIZE + p.chunkPos
	bufLen := c_CHUNKSIZE - p.chunkPos
	return slabs[s].memory[bufStart : bufStart+bufLen]
}

// appendChunk adds a chunk to the chain after the "after" chunk
func appendChunk(after uint32) (newChunk uint32) {
	if getNextLocation(after) != emptyLocation {
		panic("ASSERT!")
	}

	newChunk = grabChunk()
	setNextLocation(newChunk, getNextLocation(after))
	setNextLocation(after, newChunk)
	return newChunk
}

type byteSlab struct {
	memory []byte
	next   []uint32
	used   []bool // Only used for ASSERT checking, should be removed
	free   int

	touched time.Time
}

func allocateSlab() (ix uint16) {
	slab := &byteSlab{
		memory: make([]byte, c_SLABSIZE),
		next:   make([]uint32, c_SLABSIZE/c_CHUNKSIZE),
		used:   make([]bool, c_SLABSIZE/c_CHUNKSIZE), // Only used for ASSERT checking, should be removed
	}
	ix = 1
	for ; slabs[ix] != nil; ix++ {
	}
	slabs[ix] = slab
	allocatedSlabs++

	for i, _ := range slab.next {
		slabs[ix].used[i] = false
		slabs[ix].free++

		release := setChunkLocation(uint16(ix), uint16(i))
		setNextLocation(release, freeChunk)
		freeChunk = release
	}
	return ix
}
func deallocateSlab(ix uint16) {
	for i := range slabs[ix].used {
		if slabs[ix].used[i] {
			panic("ASSERT: Deallocate on a slab that has a chunk in use")
		}
	}

	this, last := freeChunk, emptyLocation
	freeChunk = emptyLocation
	for ; this != emptyLocation || last != emptyLocation; this = getNextLocation(this) {
		s, _ := getChunkLocation(this)
		if s != ix {
			if last != emptyLocation {
				setNextLocation(last, this)
			}
			last = this
			if freeChunk == emptyLocation {
				freeChunk = this
			}
		} else {
			slabs[ix].free--
		}
	}

	if slabs[ix].free > 0 {
		panic("ASSERT: Unable to remove all chunks from free-chain")
	}
	slabs[ix] = nil
	allocatedSlabs--
}

// grab a free chunk
func grabChunk() uint32 {
	memoryMutex.Lock()
	defer memoryMutex.Unlock()

	if freeChunk == emptyLocation {
		allocateSlab()
	}

	grabbed := freeChunk
	freeChunk = getNextLocation(grabbed)
	grabbedChunks++

	s, i := getChunkLocation(grabbed)
	if slabs[s].used[i] {
		panic(fmt.Sprintf("ASSERT: Grabbing chunk already in use %x", grabbed))
	}
	slabs[s].used[i] = true
	slabs[s].free--
	slabs[s].touched = time.Now()

	setNextLocation(grabbed, emptyLocation)
	return grabbed
}

func releaseChunk(release uint32) {
	memoryMutex.Lock()
	defer memoryMutex.Unlock()

	s, i := getChunkLocation(release)
	if !slabs[s].used[i] {
		panic(fmt.Sprintf("ASSERT: Releasing chunk not in use %x", release))
	}
	slabs[s].used[i] = false
	slabs[s].free++
	slabs[s].touched = time.Now()

	setNextLocation(release, freeChunk)
	freeChunk = release
	releasedChunks++
}

// Not really a GC, more of a slab releaser in case it has not been used for a while.
func GC() {
	for { // ever
		time.Sleep(1 * time.Minute)
		func() {
			memoryMutex.Lock()
			defer memoryMutex.Unlock()

			if allocatedSlabs > 0 {
				for s := range slabs {
					//if slabs[s] != nil {
					//	fmt.Printf("slab %d, free %d, total %d, touched %.2f sec\n", s, slabs[s].free, len(slabs[s].next), time.Since(slabs[s].touched).Seconds())
					//}
					if slabs[s] != nil && slabs[s].free == len(slabs[s].next) && time.Since(slabs[s].touched).Seconds() > 74 {
						deallocateSlab(uint16(s))
						runtime.GC()
					}
				}
			}
		}()
	}
}
func init() {
	go GC()
}

func Stats() (AllocatedSlabs int64, GrabbedChunks int64, ReleasedChunks int64, MemoryAllocated int64, MemoryInUse int64) {
	memoryMutex.Lock()
	defer memoryMutex.Unlock()

	AllocatedSlabs = int64(allocatedSlabs)
	GrabbedChunks = grabbedChunks
	ReleasedChunks = releasedChunks
	MemoryInUse = (GrabbedChunks - ReleasedChunks) * c_CHUNKSIZE
	MemoryAllocated = AllocatedSlabs * c_SLABSIZE
	return
}

func ChunkQuantize(size int) int {
	return c_CHUNKSIZE + (size/c_CHUNKSIZE)*c_CHUNKSIZE
}

func MemoryStats() string {
	a, _, _, m, u := Stats()
	return fmt.Sprintf("Memory stats: %d slabs, %s allocated, %s used", a, ShortHumanSize(m), ShortHumanSize(u))
}
