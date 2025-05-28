package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/timescale/tsbs/pkg/query"
)

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	client     http.Client
	Host       []byte
	HostString string
	uri        []byte
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Debug          int
	PrintResponses bool
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(host string) *HTTPClient {
	return &HTTPClient{
		client:     http.Client{},
		Host:       []byte(host),
		HostString: host,
		uri:        []byte{}, // heap optimization
	}
}

// Do performs the action specified by the given Query.
func (w *HTTPClient) Do(q *query.HTTP, opts *HTTPClientDoOptions) (lag float64, err error) {
	// Build URI
	w.uri = w.uri[:0]
	w.uri = append(w.uri, w.Host...)
	w.uri = append(w.uri, q.Path...)

	// Create request
	req, err := http.NewRequest(string(q.Method), string(w.uri), bytes.NewReader(q.Body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Perform request
	start := time.Now()
	resp, err := w.client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		panic("http request did not return status 200 OK")
	}

	// Read the body once
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // ms

	if opts != nil {
		// Debug printing
		switch opts.Debug {
		case 1:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms\n", q.HumanLabel, lag)
		case 2:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		case 3:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
		case 4:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
			fmt.Fprintf(os.Stderr, "debug:   response: %s\n", string(body))
		}

		// Pretty print JSON
		if opts.PrintResponses {
			var parsed interface{}
			if err := json.Unmarshal(body, &parsed); err != nil {
				// fallback to raw print
				fmt.Fprintln(os.Stderr, string(body))
			} else {
				out, _ := json.MarshalIndent(parsed, "", "  ")
				fmt.Fprintln(os.Stderr, string(out))
			}
		}
	}

	return lag, err
}
