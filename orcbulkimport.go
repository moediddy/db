package main

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// For older go releases (specifically 1.2 and earlier) there is an issue with
// COMODO certificates since they sign with sha384 which is not included by
// default. As such we force include the package to get the support. On newer
// golang installs this does nothing.
// For more information see this blog post:
//  http://bridge.grumpy-troll.org/2014/05/golang-tls-comodo/
import _ "crypto/sha512"

var (
	apiKey                = flag.String("key", "00000000-0000-0000-0000-000000000000", "the api key")
	workerCount           = flag.Int("workers", 8, "the number of worker procs")
	host                  = flag.String("host", "api.orchestrate.io", "the Orchestrate API host to use")
	reqs                  = make(chan Request, 100)
	dialTimeout           = 3 * time.Second
	responseHeaderTimeout = 60 * time.Second
	wg                    sync.WaitGroup
	client                *http.Client
)

type Request struct {
	reader   io.Reader
	respChan chan Response
}

type Response struct {
	body  map[string]interface{}
	err   *error
	eof   bool
	total int
}

func main() {
	flag.Parse()

	client = &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost:   *workerCount,
		ResponseHeaderTimeout: responseHeaderTimeout,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, dialTimeout)
		},
	}}

	startRequestHandlerPool()

	for _, file := range flag.Args() {
		wg.Add(1)
		go func(file string) {
			importFile(file)
		}(file)
	}

	wg.Wait()
	close(reqs)
}

func hello(res http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(res, "Hello, Orchestrate")
}

func startRequestHandlerPool() {
	for i := 0; i < *workerCount; i++ {
		go handleRequests(reqs)
	}
}

func importFile(filename string) {
	file, err := os.Open(filename)

	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}
	defer file.Close()

	stats, _ := file.Stat()
	fileSize := stats.Size()

	log.Printf("Importing %v", filename)

	reader := bufio.NewReaderSize(file, 1024*1024)

	var resps = make(chan Response, 100)
	go handleResponses(filename, fileSize, resps)

	var pReader *io.PipeReader
	var pWriter *io.PipeWriter
	var i int
	for i = 0; err == nil; i++ {
		if i%250 == 0 {
			if pWriter != nil {
				pWriter.Close()
			}
			pReader, pWriter = io.Pipe()
			reqs <- Request{pReader, resps}
		}

		var line []byte
		line, err = reader.ReadBytes('\n')
		pWriter.Write(line)
	}

	if pWriter != nil {
		pWriter.Close()
	}

	if err != nil && err != io.EOF {
		log.Panicf("Scanner error: %v\n", err)
	}

	resps <- Response{nil, nil, true, i-1}
}

func handleRequests(reqs chan Request) {
	for req := range reqs {
		var err error

		body := make(map[string]interface{})

		if resp, err := jsonReply("POST", "", req.reader, 200, &body); err != nil {
			log.Printf("Error %v %v\n", err, resp)
			continue
		}

		req.respChan <- Response{body, &err, false, 0}
	}
}

func handleResponses(filename string, fileSize int64, resps chan Response) {
	var importCount, errorCount, totalCount int
	eof := false

	for resp := range resps {
		if resp.eof {
			eof = true
			totalCount = resp.total
		}

		if resp.err != nil {
			switch err := (*resp.err).(type) {
			case OrchestrateError:
				errorCount++
				log.Printf("Error: %v", err)
			}
		}

		if resp.body != nil {

			if resp.body["status"] != "success" {
				log.Printf("%v: %v", resp.body["status"], resp.body["message"])

				for _, result := range resp.body["results"].([]interface{}) {
					resultMap := result.(map[string]interface{})
					if resultMap["status"].(string) == "failure" {
						log.Printf("Item failure: %v", resultMap["error"])
						errorCount++
					}
				}
			}

			importCount += int(resp.body["success_count"].(float64))
		}

		if importCount%1000 == 0 {
			log.Printf("Progress imported %v items from %v", importCount, filename)
		}

		if eof && importCount >= totalCount - errorCount {
			close(resps)
		}
	}

	log.Printf("Done importing %v items from %v (with %v errors)", importCount, filename, errorCount)

	wg.Done()
}

// Executes an HTTP request.
func doRequest(
	method, trailing string, headers map[string]string, body io.Reader,
) (*http.Response, error) {
	url := "https://" + *host + "/v0/" + trailing

	// Create the new Request.
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	// Ensure that the query gets the authToken as username.
	req.SetBasicAuth(*apiKey, "")

	// Add any headers that the client provided.
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	req.Header.Add("User-Agent", "orcbulkimport")

	req.Header.Add("Content-Type", "application/orchestrate-export-stream+json")

	return client.Do(req)
}

// This call will perform a request which expects a JSON body to be returned.
// The contents of the body will be decoded into the value given.
//
// Any status return other than 'status' will cause an error to be returned
// from this function.
func jsonReply(
	method, path string, body io.Reader, status int, value interface{},
) (*http.Response, error) {
	resp, err := doRequest(method, path, nil, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Ensure that the returned status was expected.
	if resp.StatusCode != status {
		return nil, newError(resp)
	}

	// See what kind of encoding the server is replying with.
	var decoder *json.Decoder
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		gzipReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		decoder = json.NewDecoder(gzipReader)
	case "deflate":
		decoder = json.NewDecoder(flate.NewReader(resp.Body))
	default:
		decoder = json.NewDecoder(resp.Body)
	}

	// Decode the body into a json object.
	if err := decoder.Decode(value); err != nil {
		return nil, err
	}

	// Success!
	return resp, nil
}

func newError(resp *http.Response) error {
	// We need to ensure that the body is read no matter what otherwise
	// connections won't be reused.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	oe := &OrchestrateError{
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
	}
	if err := json.Unmarshal(body, oe); err != nil {
		oe.Message = string(body)
		return oe
	}

	return oe
}

type OrchestrateError struct {
	// The status string returned from the HTTP call.
	Status string `json:"-"`

	// The status, as an integer, returned from the HTTP call.
	StatusCode int `json:"-"`

	// The Orchestrate specific message representing the error.
	Message string `json:"message"`
}

// Convert the error to a meaningful string.
func (e OrchestrateError) Error() string {
	return fmt.Sprintf("%s (%d): %s", e.Status, e.StatusCode, e.Message)
}
