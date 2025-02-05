// s3-benchmark.go
// Copyright (c) 2017 Wasabi Technology, Inc.

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Global variables
var (
	accessKey, secretKey, urlHost, bucket, region string

	durationSecs, threads, loops int
	objectSize                   uint64
	objectData                   []byte
	objectDataMd5                string
	runningThreads               int32

	listVerRowsCount, listObjRowsCount                                                uint64
	uploadCount, downloadCount, deleteCount, listVerCount, listObjCount               int32
	endTime, uploadFinish, downloadFinish, deleteFinish, listVerFinish, listObjFinish time.Time

	uploadSlowdownCount, downloadSlowdownCount, deleteSlowdownCount, listVerSlowdownCount, listObjSlowdownCount int32
)

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}

// Our HTTP transport used for the roundtripper below
var HTTPTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 0,
	// Allow an unlimited number of idle connections
	MaxIdleConnsPerHost: 4096,
	MaxIdleConns:        0,
	// But limit their idle time
	IdleConnTimeout: time.Minute,
	// Ignore TLS errors
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

var httpClient = &http.Client{Transport: HTTPTransport}

func getS3Client() *s3.S3 {
	// Build our config
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	loglevel := aws.LogOff
	// Build the rest of the configuration
	awsConfig := &aws.Config{
		Region:               aws.String(region),
		Endpoint:             aws.String(urlHost),
		Credentials:          creds,
		LogLevel:             &loglevel,
		S3ForcePathStyle:     aws.Bool(true),
		S3Disable100Continue: aws.Bool(true),
		// Comment following to use default transport
		HTTPClient: &http.Client{Transport: HTTPTransport},
	}
	session := session.New(awsConfig)
	client := s3.New(session)
	if client == nil {
		log.Fatalf("FATAL: Unable to create new client.")
	}
	// Return success
	return client
}

func createBucket(ignore_errors bool) {
	// Get a client
	client := getS3Client()
	// Create our bucket (may already exist without error)
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if _, err := client.CreateBucket(in); err != nil {
		if ignore_errors {
			log.Printf("WARNING: createBucket %s error, ignoring %v", bucket, err)
		} else {
			log.Fatalf("FATAL: Unable to create bucket %s (is your access and secret correct?): %v", bucket, err)
		}
	}
}

func deleteAllObjects() {
	// Get a client
	client := getS3Client()
	// Use multiple routines to do the actual delete
	var doneDeletes sync.WaitGroup
	// Loop deleting our versions reading as big a list as we can
	var keyMarker, versionId *string
	var err error
	for loop := 1; ; loop++ {
		// Delete all the existing objects and versions in the bucket
		in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), KeyMarker: keyMarker, VersionIdMarker: versionId, MaxKeys: aws.Int64(1000)}
		if listVersions, listErr := client.ListObjectVersions(in); listErr == nil {
			delete := &s3.Delete{Quiet: aws.Bool(true)}
			for _, version := range listVersions.Versions {
				delete.Objects = append(delete.Objects, &s3.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
			}
			for _, marker := range listVersions.DeleteMarkers {
				delete.Objects = append(delete.Objects, &s3.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
			}
			if len(delete.Objects) > 0 {
				// Start a delete routine
				doDelete := func(bucket string, delete *s3.Delete) {
					if _, e := client.DeleteObjects(&s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: delete}); e != nil {
						err = fmt.Errorf("DeleteObjects unexpected failure: %s", e.Error())
					}
					doneDeletes.Done()
				}
				doneDeletes.Add(1)
				go doDelete(bucket, delete)
			}
			// Advance to next versions
			if listVersions.IsTruncated == nil || !*listVersions.IsTruncated {
				break
			}
			keyMarker = listVersions.NextKeyMarker
			versionId = listVersions.NextVersionIdMarker
		} else {
			// The bucket may not exist, just ignore in that case
			if strings.HasPrefix(listErr.Error(), "NoSuchBucket") {
				return
			}
			err = fmt.Errorf("ListObjectVersions unexpected failure: %v", listErr)
			break
		}
	}
	// Wait for deletes to finish
	doneDeletes.Wait()
	// If error, it is fatal
	if err != nil {
		log.Fatalf("FATAL: Unable to delete objects from bucket: %v", err)
	}
}

// canonicalAmzHeaders -- return the x-amz headers canonicalized
func canonicalAmzHeaders(req *http.Request) string {
	// Parse out all x-amz headers
	var headers []string
	for header := range req.Header {
		norm := strings.ToLower(strings.TrimSpace(header))
		if strings.HasPrefix(norm, "x-amz") {
			headers = append(headers, norm)
		}
	}
	// Put them in sorted order
	sort.Strings(headers)
	// Now add back the values
	for n, header := range headers {
		headers[n] = header + ":" + strings.Replace(req.Header.Get(header), "\n", " ", -1)
	}
	// Finally, put them back together
	if len(headers) > 0 {
		return strings.Join(headers, "\n") + "\n"
	} else {
		return ""
	}
}

func hmacSHA1(key []byte, content string) []byte {
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(content))
	return mac.Sum(nil)
}

func setSignature(req *http.Request) {
	// Setup default parameters
	dateHdr := time.Now().UTC().Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", dateHdr)
	// Get the canonical resource and header
	canonicalResource := req.URL.EscapedPath()
	canonicalHeaders := canonicalAmzHeaders(req)
	stringToSign := req.Method + "\n" + req.Header.Get("Content-MD5") + "\n" + req.Header.Get("Content-Type") + "\n\n" +
		canonicalHeaders + canonicalResource
	hash := hmacSHA1([]byte(secretKey), stringToSign)
	signature := base64.StdEncoding.EncodeToString(hash)
	req.Header.Set("Authorization", fmt.Sprintf("AWS %s:%s", accessKey, signature))
}

func runUpload(thread_num int) {
	for time.Now().Before(endTime) {
		objnum := atomic.AddInt32(&uploadCount, 1)
		fileobj := bytes.NewReader(objectData)
		prefix := fmt.Sprintf("%s/%s/Object-%d", urlHost, bucket, objnum)
		req, _ := http.NewRequest("PUT", prefix, fileobj)
		req.Header.Set("Content-Length", strconv.FormatUint(objectSize, 10))
		req.Header.Set("Content-MD5", objectDataMd5)
		setSignature(req)
		if resp, err := httpClient.Do(req); err != nil {
			log.Fatalf("FATAL: Error uploading object %s: %v", prefix, err)
		} else if resp != nil && resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusServiceUnavailable {
				atomic.AddInt32(&uploadSlowdownCount, 1)
				atomic.AddInt32(&uploadCount, -1)
			} else {
				fmt.Printf("Upload status %s: resp: %+v\n", resp.Status, resp)
				if resp.Body != nil {
					body, _ := ioutil.ReadAll(resp.Body)
					fmt.Printf("Body: %s\n", string(body))
				}
			}
		}
	}
	// Remember last done time
	uploadFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func runDownload(thread_num int) {
	for time.Now().Before(endTime) {
		atomic.AddInt32(&downloadCount, 1)
		objnum := rand.Int31n(downloadCount) + 1
		prefix := fmt.Sprintf("%s/%s/Object-%d", urlHost, bucket, objnum)
		req, _ := http.NewRequest("GET", prefix, nil)
		setSignature(req)
		if resp, err := httpClient.Do(req); err != nil {
			log.Fatalf("FATAL: Error downloading object %s: %v", prefix, err)
		} else if resp != nil && resp.Body != nil {
			if resp.StatusCode == http.StatusServiceUnavailable {
				atomic.AddInt32(&downloadSlowdownCount, 1)
				atomic.AddInt32(&downloadCount, -1)
			} else {
				io.Copy(ioutil.Discard, resp.Body)
			}
		}
	}
	// Remember last done time
	downloadFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func runListingVersions(thread_num int) {
	var keyMarker, versionId, delimiter *string
	objnum := rand.Int31n(downloadCount) + 1
	prefix := fmt.Sprintf(`Object-%d`, objnum%100)
	client := getS3Client()
	delimiterCounter := 0
	for time.Now().Before(endTime) {
		atomic.AddInt32(&listVerCount, 1)
		in := &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucket),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionId,
			MaxKeys:         aws.Int64(1000),
			Prefix:          &prefix,
			Delimiter:       delimiter,
		}
		res, err := client.ListObjectVersions(in)
		if err != nil {
			atomic.AddInt32(&listVerSlowdownCount, 1)
			atomic.AddInt32(&listVerCount, -1)
			log.Printf(`WARNING: failed %v %s`, in, err)
		}
		if res != nil {
			total := uint64(len(res.Versions) + len(res.CommonPrefixes))
			atomic.AddUint64(&listVerRowsCount, total)
		}
		if res == nil || len(res.Versions) == 0 || res.KeyMarker == nil || res.NextKeyMarker == nil {
			objnum = rand.Int31n(downloadCount) + 1
			prefix = fmt.Sprintf(`Object-%d`, objnum%100)
			delimiterCounter++
			delimiterCounter %= 10
			if delimiterCounter > 7 {
				delimiter = nil
			} else {
				delimiter = aws.String(fmt.Sprint(delimiterCounter))
			}
		} else {
			keyMarker = res.NextKeyMarker
			versionId = res.NextVersionIdMarker
		}
	}
	// Remember last done time
	listVerFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func runListObjectsV2(thread_num int) {
	var continuationToken, delimiter *string
	objnum := rand.Int31n(downloadCount) + 1
	prefix := fmt.Sprintf(`Object-%d`, objnum%100)
	client := getS3Client()
	delimiterCounter := 0
	for time.Now().Before(endTime) {
		atomic.AddInt32(&listObjCount, 1)
		in := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			MaxKeys:           aws.Int64(1000),
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
			Delimiter:         delimiter,
		}
		res, err := client.ListObjectsV2(in)
		if err != nil {
			atomic.AddInt32(&listObjSlowdownCount, 1)
			atomic.AddInt32(&listObjCount, -1)
			log.Printf(`WARNING: failed %v %s`, in, err)
		}
		if res != nil {
			total := uint64(len(res.Contents) + len(res.CommonPrefixes))
			atomic.AddUint64(&listObjRowsCount, total)
		}
		if res == nil || len(res.Contents) == 0 || res.NextContinuationToken == nil {
			objnum = rand.Int31n(downloadCount) + 1
			prefix = fmt.Sprintf(`Object-%d`, objnum%100)
			delimiterCounter++
			delimiterCounter %= 10
			if delimiterCounter > 7 {
				delimiter = nil
			} else {
				delimiter = aws.String(fmt.Sprint(delimiterCounter))
			}
		} else {
			continuationToken = res.NextContinuationToken
		}
	}
	// Remember last done time
	listObjFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func runDelete(thread_num int) {
	for {
		objnum := atomic.AddInt32(&deleteCount, 1)
		if objnum > uploadCount {
			break
		}
		prefix := fmt.Sprintf("%s/%s/Object-%d", urlHost, bucket, objnum)
		req, _ := http.NewRequest("DELETE", prefix, nil)
		setSignature(req)
		if resp, err := httpClient.Do(req); err != nil {
			log.Fatalf("FATAL: Error deleting object %s: %v", prefix, err)
		} else if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
			atomic.AddInt32(&deleteSlowdownCount, 1)
			atomic.AddInt32(&deleteCount, -1)
		}
	}
	// Remember last done time
	deleteFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func main() {
	// Hello
	fmt.Println("Wasabi benchmark program v2.0")

	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.StringVar(&accessKey, "a", "", "Access key")
	myflag.StringVar(&secretKey, "s", "", "Secret key")
	myflag.StringVar(&urlHost, "u", "http://s3.wasabisys.com", "URL for host with method prefix")
	myflag.StringVar(&bucket, "b", "wasabi-benchmark-bucket", "Bucket for testing")
	myflag.StringVar(&region, "r", "us-east-1", "Region for testing")
	myflag.IntVar(&durationSecs, "d", 60, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	if accessKey == "" {
		log.Fatal("Missing argument -a for access key.")
	}
	if secretKey == "" {
		log.Fatal("Missing argument -s for secret key.")
	}
	var err error
	if objectSize, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	// Echo the parameters
	logit(fmt.Sprintf("Parameters: url=%s, bucket=%s, region=%s, duration=%d, threads=%d, loops=%d, size=%s",
		urlHost, bucket, region, durationSecs, threads, loops, sizeArg))

	// Initialize data for the bucket
	objectData = make([]byte, objectSize)
	rand.Read(objectData)
	hasher := md5.New()
	hasher.Write(objectData)
	objectDataMd5 = base64.StdEncoding.EncodeToString(hasher.Sum(nil))

	// Create the bucket and delete all the objects
	createBucket(true)
	deleteAllObjects()

	// Loop running the tests
	for loop := 1; loop <= loops; loop++ {

		// reset counters
		uploadCount = 0
		uploadSlowdownCount = 0
		downloadCount = 0
		downloadSlowdownCount = 0
		deleteCount = 0
		deleteSlowdownCount = 0

		// Run the upload case
		{
			runningThreads = int32(threads)
			startTime := time.Now()
			endTime = startTime.Add(time.Second * time.Duration(durationSecs))
			for n := 1; n <= threads; n++ {
				go runUpload(n)
			}

			// Wait for it to finish
			for atomic.LoadInt32(&runningThreads) > 0 {
				time.Sleep(time.Millisecond)
			}
			upload_time := uploadFinish.Sub(startTime).Seconds()

			bps := float64(uint64(uploadCount)*objectSize) / upload_time
			logit(fmt.Sprintf("Loop %d: PUT time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
				loop, upload_time, uploadCount, bytefmt.ByteSize(uint64(bps)), float64(uploadCount)/upload_time, uploadSlowdownCount))
		}

		// Run the download case
		{
			runningThreads = int32(threads)
			startTime := time.Now()
			endTime = startTime.Add(time.Second * time.Duration(durationSecs))
			for n := 1; n <= threads; n++ {
				go runDownload(n)
			}

			// Wait for it to finish
			for atomic.LoadInt32(&runningThreads) > 0 {
				time.Sleep(time.Millisecond)
			}
			downloadTime := downloadFinish.Sub(startTime).Seconds()
			bps := float64(uint64(downloadCount)*objectSize) / downloadTime

			logit(fmt.Sprintf("Loop %d: GET time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
				loop, downloadTime, downloadCount, bytefmt.ByteSize(uint64(bps)), float64(downloadCount)/downloadTime, downloadSlowdownCount))
		}

		// Run the list objects v2 case
		{
			runningThreads = int32(threads)
			startTime := time.Now()
			endTime = startTime.Add(time.Second * time.Duration(durationSecs))
			for n := 1; n <= threads; n++ {
				go runListObjectsV2(n)
			}
			// Wait for it to finish
			for atomic.LoadInt32(&runningThreads) > 0 {
				time.Sleep(time.Millisecond)
			}
			listingTime := listObjFinish.Sub(startTime).Seconds()
			rowsPerSec := float64(listObjRowsCount) / listingTime
			opsPerSec := float64(listObjCount) / listingTime

			logit(fmt.Sprintf("Loop %d: LIST2 time %.1f secs, ops = %d, speed = %.1f rows/sec, %.1f operations/sec. Slowdowns = %d",
				loop, listingTime, listObjCount, rowsPerSec, opsPerSec, listObjSlowdownCount))
		}

		// Run the list object versions case
		{
			runningThreads = int32(threads)
			startTime := time.Now()
			endTime = startTime.Add(time.Second * time.Duration(durationSecs))
			for n := 1; n <= threads; n++ {
				go runListingVersions(n)
			}
			// Wait for it to finish
			for atomic.LoadInt32(&runningThreads) > 0 {
				time.Sleep(time.Millisecond)
			}
			listingTime := listVerFinish.Sub(startTime).Seconds()
			rowsPerSec := float64(listVerRowsCount) / listingTime
			opsPerSec := float64(listVerCount) / listingTime

			logit(fmt.Sprintf("Loop %d: LISTver time %.1f secs, ops = %d, speed = %.1f rows/sec, %.1f operations/sec. Slowdowns = %d",
				loop, listingTime, listVerCount, rowsPerSec, opsPerSec, listVerSlowdownCount))
		}

		// Run the delete case
		{
			runningThreads = int32(threads)
			startTime := time.Now()
			endTime = startTime.Add(time.Second * time.Duration(durationSecs))
			for n := 1; n <= threads; n++ {
				go runDelete(n)
			}

			// Wait for it to finish
			for atomic.LoadInt32(&runningThreads) > 0 {
				time.Sleep(time.Millisecond)
			}
			deleteTime := deleteFinish.Sub(startTime).Seconds()

			logit(fmt.Sprintf("Loop %d: DELETE time %.1f secs, %.1f deletes/sec. Slowdowns = %d",
				loop, deleteTime, float64(uploadCount)/deleteTime, deleteSlowdownCount))
		}
	}

	// All done
}
