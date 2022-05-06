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

package filters

import (
	"net/http"
	"strings"

	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// WithCompression returns a http.Handler that performs response compression. This is enabled only when
// APIResponseCompression feature gate is turned on. Note: We only support gzip today and its compression
// level is configured through the responseGzipCompressionLevel parameter.
func WithCompression(handler http.Handler, minResponseSizeBytesForCompression, responseGzipCompressionLevel int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		contentEncoding := negotiateContentEncoding(req)
		switch contentEncoding {
		case responsewriters.GzipEncoding:
			gw := responsewriters.NewGzipResponseWriter(w, minResponseSizeBytesForCompression, responseGzipCompressionLevel)
			handler.ServeHTTP(gw, req)
		default:
			handler.ServeHTTP(w, req)
		}
	})
}

// negotiateContentEncoding returns a supported client-requested content encoding for the
// provided request. It will return the empty string if no supported content encoding was
// found. We only support gzip today.
func negotiateContentEncoding(req *http.Request) string {
	encoding := req.Header.Get(responsewriters.AcceptEncoding)
	for len(encoding) > 0 {
		var token string
		if next := strings.Index(encoding, ","); next != -1 {
			token = encoding[:next]
			encoding = encoding[next+1:]
		} else {
			token = encoding
			encoding = ""
		}
		switch strings.TrimSpace(token) {
		case responsewriters.GzipEncoding:
			return responsewriters.GzipEncoding
		}
	}
	return ""
}
