//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bufio"
	"os"
)

//********************************************************************************//
//                                  BufferedFile                                  //
//********************************************************************************//

type BufferedReader struct {
	File *os.File
	*bufio.Reader
}

func OpenBufferedReader(path string, buffersize int, flag int) (*BufferedReader, error) {
	file, err := os.OpenFile(path, flag|os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return &BufferedReader{file, bufio.NewReaderSize(file, buffersize)}, nil
}
func (b *BufferedReader) Seek(offset int64, whence int) (ret int64, err error) {
	ret, err = b.File.Seek(offset, whence)
	if err == nil {
		b.Reset(b.File)
	}
	return ret, err
}

type BufferedWriter struct {
	File *os.File
	*bufio.Writer
}

func OpenBufferedWriter(path string, buffersize int, flag int, perm os.FileMode) (*BufferedWriter, error) {
	file, err := os.OpenFile(path, flag|os.O_WRONLY, perm)
	if err != nil {
		return nil, err
	}
	return &BufferedWriter{file, bufio.NewWriterSize(file, buffersize)}, nil
}
func (b *BufferedWriter) Seek(offset int64, whence int) (ret int64, err error) {
	b.Flush() // Always flush in case we want to read what we have written
	ret, err = b.File.Seek(offset, whence)
	if err == nil {
		b.Reset(b.File)
	}
	return ret, err
}

type BufferedFile struct {
	Path       string
	BufferSize int
	Flag       int
	Perm       os.FileMode
	Reader     *BufferedReader
	Writer     *BufferedWriter
}

func OpenBufferedFile(path string, buffersize int, flag int, perm os.FileMode) (*BufferedFile, error) {
	b := &BufferedFile{Path: path, BufferSize: buffersize, Flag: flag, Perm: perm}
	var err error
	if b.Reader, err = OpenBufferedReader(b.Path, b.BufferSize, b.Flag); err != nil {
		return nil, err
	}
	if b.Writer, err = OpenBufferedWriter(b.Path, b.BufferSize, b.Flag, b.Perm); err != nil {
		return nil, err
	}
	return b, nil
}
func (b *BufferedFile) Size() int64 {
	b.Writer.Flush() // Always flush in case we want to read what we have written
	info, err := b.Reader.File.Stat()
	PanicOn(err)
	return info.Size()
}
func (b *BufferedFile) Close() (err error) {
	b.Writer.Flush() // Always flush in case we want to read what we have written
	if e := b.Reader.File.Close(); e != nil {
		err = e
	}
	if e := b.Writer.File.Close(); e != nil {
		err = e
	}
	return err
}
