/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package responsewriters

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
)

const (
	AcceptEncoding        = "Accept-Encoding"
	headerContentEncoding = "Content-Encoding"
	headerVary            = "Vary"
	GzipEncoding          = "gzip"
)

var (
	// gzipPool maintains a pool of gzip writers for response compression. It provides better performance by
	// reusing the writers across HTTP requests rather than allocating (and GC'ing) a new one for each request.
	gzipPool *sync.Pool
)

// CompressingResponseWriter defines a generic response writer that performs response compression.
type CompressingResponseWriter interface {
	http.ResponseWriter
	http.CloseNotifier
	http.Flusher
	http.Hijacker
	responsewriter.UserProvidedDecorator

	// CompressionRecommended suggests if the given bytes of response need compression. The caller of this method can
	// use it to decide whether to compress the response or not. To actually compress the response, EnableCompression
	// method below must be called.
	CompressionRecommended(p []byte) bool

	// EnableCompression tells the gzipResponseWriter to enable response compression. The caller MUST call this method
	// before invoking WriteHeader() since we set some encoding-related headers here that need to be sent to the client.
	EnableCompression()
}

// gzipResponseWriter implements a gzip-type response compressor.
type gzipResponseWriter struct {
	http.ResponseWriter
	w                                        io.Writer
	recommendedMinResponseSizeForCompression int
	shouldCompress                           bool
	hasWritten                               bool
}

func NewGzipResponseWriter(w http.ResponseWriter, minResponseSizeBytesForCompression, responseGzipCompressionLevel int) CompressingResponseWriter {
	// Initialize the gzip writer pool if it hasn't been yet.
	if gzipPool == nil {
		gzipPool = &sync.Pool{
			New: func() interface{} {
				gw, err := gzip.NewWriterLevel(nil, responseGzipCompressionLevel)
				if err != nil {
					panic(err)
				}
				return gw
			},
		}
	}

	return &gzipResponseWriter{
		ResponseWriter:                           w,
		recommendedMinResponseSizeForCompression: minResponseSizeBytesForCompression,
	}
}

func (g *gzipResponseWriter) CompressionRecommended(p []byte) bool {
	// TODO: Can we make below recommendation smarter by taking into account the entropy of the data too?
	return len(p) >= g.recommendedMinResponseSizeForCompression
}

func (g *gzipResponseWriter) EnableCompression() {
	if g.hasWritten {
		// We can't revisit decision to compress once the initial write has happened already.
		return
	}

	g.shouldCompress = true
	header := g.ResponseWriter.Header()
	header.Set(headerContentEncoding, GzipEncoding)
	header.Add(headerVary, AcceptEncoding)
	// We don't fetch a gzip-writer from the pool here yet to avoid unnecessarily holding one till we actually
	// perform the response Write(). We don't even need it for writing the header.
}

func (g *gzipResponseWriter) Write(p []byte) (int, error) {
	// Below helps avoid revisiting decision to compress once the initial write has happened already.
	g.hasWritten = true

	if g.w == nil {
		if g.shouldCompress {
			gzipWriter := gzipPool.Get().(*gzip.Writer)
			gzipWriter.Reset(g.ResponseWriter)
			g.w = gzipWriter
		} else {
			g.w = g.ResponseWriter
		}
	}

	// Release underlying gzipWriter after each write to avoid holding it up for longer durations (e.g watch)
	defer func() {
		if err := g.Close(); err != nil {
			utilruntime.HandleError(fmt.Errorf("gzip response writer failed to close cleanly: %v", err))
		}
	}()

	return g.w.Write(p)
}

func (g *gzipResponseWriter) Flush() {
	if g.w == nil {
		return
	}

	switch gw := g.w.(type) {
	case *gzip.Writer:
		// Flush the compressed gzip data to the underlying writer.
		if err := gw.Flush(); err != nil {
			utilruntime.HandleError(fmt.Errorf("gzip response writer failed to flush the response: %v", err))
		}
	default:
		gw.(http.Flusher).Flush()
	}

}

func (g *gzipResponseWriter) Close() error {
	if g.w == nil {
		return nil
	}

	var err error
	switch gw := g.w.(type) {
	case *gzip.Writer:
		// Release the gzip.Writer back to the pool for reuse.
		err = gw.Close()
		gw.Reset(nil)
		gzipPool.Put(gw)
	}
	g.w = nil
	return err
}

func (g *gzipResponseWriter) Unwrap() http.ResponseWriter {
	return g.ResponseWriter
}

func (g *gzipResponseWriter) CloseNotify() <-chan bool {
	// If a CloseNotify is performed on this writer, delegate to the inner writer
	// assuming blindly that it implements http.CloseNotifier.
	return g.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

func (g *gzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	// If a Hijack is performed on this writer, delegate to the inner writer
	// assuming blindly that it implements http.Hijacker.
	return g.ResponseWriter.(http.Hijacker).Hijack()
}
