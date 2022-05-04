package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apoorvam/goterminal"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kokizzu/gotro/I"
	"github.com/kokizzu/gotro/S"
)

////////////////////////////////////////////////////////////////////////////////
// Seed object
// uses murmur64 hash

type Seed uint64

func (s *Seed) Next() uint64 {
	h := *s
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	*s = h
	return uint64(h)
}

func (s *Seed) NextUuid() string {
	const on16 = 0xffff
	const on32 = 0xffffffff
	// 128 bit, so need 2 parts
	p1 := uint64(*s) // 32, 16, 16, eg. 2405a682-1362-4eed
	p2 := s.Next()   // 16, 48,     eg. 9d8a-582a62cab164
	return fmt.Sprintf(`%08x-%04x-%04x-%04x-%012x`, (p1>>32)&on32, (p1>>16)&on16, p1&on16, p2&on16, p2>>16)
}

func (s *Seed) NextHex16() (hex string) {
	// 16 digits = 64-bit uint
	h := *s
	s.Next()
	return fmt.Sprintf(`%016x`, h)
}

func (s *Seed) NextInt64() uint64 {
	h := uint64(*s)
	s.Next()
	return h
}

func (s *Seed) NextUint16s() (uint16, uint16, uint16, uint16) {
	h := uint64(*s)
	s.Next()
	return uint16(h >> 48), uint16(h >> 32), uint16(h >> 16), uint16(h)
}

const veeamPrefix = `Veeam/Archive/veeam/`
const veeamExt = `.blk`
const veeamZeroSuffix = `00000000000000000000000000000000` + veeamExt

// Veeam/Archive/veeam/2405a682-1362-4eed-9d8a-582a62cab164/2005ac25-ba22-453a-b3ed-a509ee49130f/blocks/4dcb5c69321eaac6196ce2099bc1964f/10469529.c401cbbc222c32802c257c98d107425c.00000000000000000000000000000000.blk
func (s *Seed) NextVeeamFiles(maxFolder1, maxFolder2, maxFolder3 uint16) []string {
	rand1, rand2, rand3, _ := s.NextUint16s()
	rand1 = 1 + (rand1 % maxFolder1)
	rand2 = 1 + (rand2 % maxFolder2)
	rand3 = 1 + (rand3 % maxFolder3)
	zero := uint16(0)
	res := make([]string, 0, rand1*rand2*rand3)
	for z := zero; z < rand1; z++ {
		for y := zero; y < rand2; y++ {
			for x := zero; x < rand3; x++ {
				line := fmt.Sprintf(`%s/%s/blocks/%s/%d.%s.`,
					s.NextUuid(),
					s.NextUuid(),
					s.NextHex16(),
					s.NextInt64(),
					s.NextHex16())
				if x == 0 {
					line += veeamZeroSuffix
				} else {
					line += s.NextHex16() + veeamExt
				}
				res = append(res, line)
			}
		}
	}
	return res
}

////////////////////////////////////////////////////////////////////////////////
// flag parser for benchmark config

type BenchConfig struct {
	Endpoint             string
	AccessKey            string
	SecretKey            string
	GoRoutineCount       int
	DurationSeconds      int
	DeltaDurationSeconds int
	InitialSeed          uint64
	MaxFolder1Capacity   uint16
	MaxFolder2Capacity   uint16
	MaxFolder3Capacity   uint16
	BucketName           string
}

func (b *BenchConfig) ParseFromArgs(args []string) (string, int) {
	l := len(args)
	if l == 0 {
		return `software to benchmark AWS S3-compatible service against VEEAM pattern

put     -------------------- 
get         --------------------
list             --------------------
delete                -------------------- 
                 |....| --> delta duration (-d)
        |..................| --> duration (-s)

usage:
  veeam-pattern ENDPOINT_URL ACCESS_KEY SECRET_KEY [other flags]

other flags:
-n goroutine count (int, default: 1, min: 1)
-s duration seconds (int, default: 60, min: 4)
-d delta duration seconds (int, default: 5, min: 1)
-r seed value (uint64, default: 1 not randomized)
-f1 maximum number of content inside 1st level uuid folder (int, default: 10, min: 2)
-f2 maximum number of content inside 2nd level uuid folder (int, default: 10, min: 2) 
-f3 maximum number of content inside 3rd level hex folder (int, default: 10, min: 2)
-b bucket name (string, default: veeam-test)

eg. UUID1/UUID2/blocks/HEX3/NUM4.HEX5.HEX6
         ^ -f1        ^ -f2  ^ -f3

so f1 x f2 x f3 = total number of objects inside UUID1 folder
`, 1
	}
	if l < 3 {
		return `require endpoint, access, and secret key as first 3 arguments`, 2
	}
	b.Endpoint = args[0]
	if !S.EndsWith(b.Endpoint, `/`) {
		b.Endpoint += `/`
	}
	if !S.StartsWith(b.Endpoint, `http`) {
		b.Endpoint = `http://` + b.Endpoint
	}
	b.AccessKey = args[1]
	b.SecretKey = args[2]

	// helper func
	u16 := func(s string, min uint16) uint16 {
		v := uint16(S.ToInt(s))
		if v < min {
			return min
		}
		return v
	}
	i := func(s string, min int) int {
		return I.MaxOf(S.ToInt(s), min)
	}

	for z := 3; z < l; z += 2 {
		key := args[z]
		if z+1 >= l {
			return `require argument for ` + key, 3
		}
		val := args[z+1]
		switch key {
		case `-n`:
			b.GoRoutineCount = i(val, 1)
		case `-s`:
			b.DurationSeconds = i(val, 4)
		case `-d`:
			b.DeltaDurationSeconds = i(val, 1)
		case `-r`:
			b.InitialSeed = I.UMax(S.ToU(val), 1)
		case `-f1`:
			b.MaxFolder1Capacity = u16(val, 2)
		case `-f2`:
			b.MaxFolder2Capacity = u16(val, 2)
		case `-f3`:
			b.MaxFolder3Capacity = u16(val, 2)
		case `-b`:
			b.BucketName = val
		}
	}
	return ``, 0
}

func (b *BenchConfig) SetDefaults() {
	b.InitialSeed = 1
	b.GoRoutineCount = 1
	b.DurationSeconds = 60
	b.DeltaDurationSeconds = 5
	b.MaxFolder1Capacity = 10
	b.MaxFolder2Capacity = 10
	b.MaxFolder3Capacity = 10
	b.BucketName = `veeam-test`
}

func (b *BenchConfig) TotalDuration() int {
	return b.DurationSeconds + 3*b.DeltaDurationSeconds
}

////////////////////////////////////////////////////////////////////////////////
// custom s3 client

type S3Client struct {
	*s3.S3
	accessKey string
	secretKey string
}

func (S3Client) canonicalAmzHeaders(req *http.Request) string {
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
func (s S3Client) hmacSHA1(key []byte, content string) []byte {
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(content))
	return mac.Sum(nil)
}

func (s S3Client) setSignature(req *http.Request) {
	// Setup default parameters
	dateHdr := time.Now().UTC().Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", dateHdr)
	// Get the canonical resource and header
	canonicalResource := req.URL.EscapedPath()
	canonicalHeaders := s.canonicalAmzHeaders(req)
	stringToSign := req.Method + "\n" + req.Header.Get("Content-MD5") + "\n" + req.Header.Get("Content-Type") + "\n\n" +
		canonicalHeaders + canonicalResource
	hash := s.hmacSHA1([]byte(s.secretKey), stringToSign)
	signature := base64.StdEncoding.EncodeToString(hash)
	req.Header.Set("Authorization", fmt.Sprintf("AWS %s:%s", s.accessKey, signature))
}

func (s *S3Client) Hit(req *http.Request) (*http.Response, error) {
	s.setSignature(req)
	return HTTPClient.Do(req)
}

////////////////////////////////////////////////////////////////////////////////
// benchmark suite

type BenchmarkSuite struct {
	PutCount  int64
	GetCount  int64
	ListCount int64
	DelCount  int64

	ListRowsCount int64

	PutErr  int64
	GetErr  int64
	ListErr int64
	DelErr  int64

	Runner []BenchmarkSteps

	Config *BenchConfig
}

func (s *BenchmarkSuite) FromConfig(b *BenchConfig) *BenchmarkSuite {
	s.Runner = make([]BenchmarkSteps, b.GoRoutineCount)
	s.Config = b
	for z := 0; z < b.GoRoutineCount; z++ {
		s.Runner[z] = BenchmarkSteps{
			PutSeed: Seed(b.InitialSeed + uint64(z)),
			Config:  b,
			Suite:   s,
		}
	}
	return s
}

// copied from wasabi
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
var HTTPClient = &http.Client{Transport: HTTPTransport}

func (s *BenchmarkSuite) CreateS3Client() S3Client {
	conf := s.Config
	creds := credentials.NewStaticCredentials(conf.AccessKey, conf.SecretKey, "")
	loglevel := aws.LogOff
	// Build the rest of the configuration
	awsConfig := &aws.Config{
		Region:               aws.String(`us-east-1`),
		Endpoint:             aws.String(conf.Endpoint),
		Credentials:          creds,
		LogLevel:             &loglevel,
		S3ForcePathStyle:     aws.Bool(true),
		S3Disable100Continue: aws.Bool(true),
		// Comment following to use default transport
		HTTPClient: &http.Client{Transport: HTTPTransport},
	}
	sess := session.New(awsConfig)
	client := s3.New(sess)
	if client == nil {
		log.Fatalf("FATAL: Unable to create new client.")
	}
	return S3Client{client, conf.AccessKey, conf.SecretKey}
}

func (s *BenchmarkSuite) Run() {
	// create bucket
	s.CreateBucket()

	// prepare runner
	wg := sync.WaitGroup{}
	l := s.Config.GoRoutineCount
	wg.Add(l)
	term := goterminal.New(os.Stderr)

	// run benchmark
	for z := 0; z < l; z++ {
		go func(z int) {
			s.Runner[z].Run(z)
			wg.Done()
		}(z)
	}

	// print progress
	toRate := func(n int64, sec float64) float64 {
		return float64(n) / sec
	}
	totalDur := s.Config.TotalDuration()
	printer := func(seconds int) string {
		sec := I.MinOf(seconds, s.Config.DurationSeconds)
		fsec := float64(sec)
		return fmt.Sprintf("%d (%.1f/s, %d err) put, %d (%.1f/s, %d err) get, %d (%.1f/s, rows=%d, %.1f rows/s, %d err) list, %d (%.1f/s, %d err) del | %.2f%%%% ~%ds\n",
			s.PutCount, toRate(s.PutCount, fsec), s.PutErr,
			s.GetCount, toRate(s.GetCount, fsec), s.GetErr,
			s.ListCount, toRate(s.ListCount, fsec),
			s.ListRowsCount, toRate(s.ListRowsCount, fsec), s.ListErr,
			s.DelCount, toRate(s.DelCount, fsec), s.DelErr,
			100*float32(seconds)/float32(totalDur), totalDur-seconds)
	}
	go func() {
		for z := 1; z <= totalDur; z++ {
			time.Sleep(time.Second)
			term.Clear()
			_, _ = fmt.Fprintf(term, printer(z))
			_ = term.Print()
		}
	}()

	// wait for finish
	wg.Wait()
	term.Clear()

	// print final result
	printer(totalDur)
	listDur := s.AverageListDuration()
	fmt.Printf(`
PUT  %5d (%4.1f/s, %d ERR)
GET  %5d (%4.1f/s, %d ERR)
LIST %5d (%4.1f/s, %d ERR, %d rows, %.1f rows/s)
DEL  %5d (%4.1f/s, %d ERR)
`,
		s.PutCount, toRate(s.PutCount, s.AveragePutDuration()), s.PutErr,
		s.GetCount, toRate(s.GetCount, s.AverageGetDuration()), s.GetErr,
		s.ListCount, toRate(s.ListCount, listDur), s.ListErr,
		s.ListRowsCount, toRate(s.ListRowsCount, listDur),
		s.DelCount, toRate(s.DelCount, s.AverageDelDuration()), s.DelErr)
}

func (s *BenchmarkSuite) CreateBucket() {
	client := s.CreateS3Client()
	bucketName := s.Config.BucketName
	in := &s3.CreateBucketInput{Bucket: aws.String(bucketName)}
	if _, err := client.CreateBucket(in); err != nil {
		log.Printf("WARNING: CreateBucket %s error, ignoring %v", bucketName, err)
	}
}

func (s *BenchmarkSuite) AveragePutDuration() (avg float64) {
	for z := range s.Runner {
		avg += float64(s.Runner[z].PutMillis)
	}
	avg /= float64(s.Config.GoRoutineCount)
	avg /= 1e3 // millis to sec
	return
}

func (s *BenchmarkSuite) AverageGetDuration() (avg float64) {
	for z := range s.Runner {
		avg += float64(s.Runner[z].GetMillis)
	}
	avg /= float64(s.Config.GoRoutineCount)
	avg /= 1e3 // millis to sec
	return
}

func (s *BenchmarkSuite) AverageListDuration() (avg float64) {
	for z := range s.Runner {
		avg += float64(s.Runner[z].ListMillis)
	}
	avg /= float64(s.Config.GoRoutineCount)
	avg /= 1e3 // millis to sec
	return
}

func (s *BenchmarkSuite) AverageDelDuration() (avg float64) {
	for z := range s.Runner {
		avg += float64(s.Runner[z].DelMillis)
	}
	avg /= float64(s.Config.GoRoutineCount)
	avg /= 1e3 // millis to sec
	return
}

func (s *BenchmarkSuite) CreateUrl(objName string) string {
	return s.Config.Endpoint + s.Config.BucketName + `/` + veeamPrefix + objName
}

////////////////////////////////////////////////////////////////////////////////
// benchmark runner

type BenchmarkSteps struct {
	PutSeed    Seed
	PutMillis  uint64
	GetMillis  uint64
	ListMillis uint64
	DelMillis  uint64

	Suite     *BenchmarkSuite
	Config    *BenchConfig
	WaitGroup sync.WaitGroup
	Objects   []string
}

func (r *BenchmarkSteps) Run(_ int) {
	// prepare progress bar
	r.WaitGroup = sync.WaitGroup{}
	deltaDur := time.Duration(r.Config.DeltaDurationSeconds) * time.Second
	r.WaitGroup.Add(4)
	go r.RunGet(deltaDur)
	go r.RunList(2 * deltaDur)
	go r.RunDel(3 * deltaDur)
	r.RunPut()
	r.WaitGroup.Wait()
}

func (r *BenchmarkSteps) RunPut() {
	defer r.MarkDuration(time.Now(), &r.PutMillis)

	cli := r.Suite.CreateS3Client()
	counter := 0
	end := time.Now().Add(time.Duration(r.Config.DurationSeconds) * time.Second)

	r.AppendObjects()
	for time.Now().Before(end) {
		atomic.AddInt64(&r.Suite.PutCount, 1)
		fileobj := bytes.NewReader([]byte{})
		for counter >= len(r.Objects) {
			r.AppendObjects()
		}
		objName := r.Suite.CreateUrl(r.Objects[counter])
		counter++
		req, _ := http.NewRequest("PUT", objName, fileobj)
		req.Header.Set("Content-Length", strconv.FormatUint(0, 10))
		if resp, err := cli.Hit(req); err != nil {
			log.Fatalf("FATAL: Error uploading object %s: %v", objName, err)
		} else if resp != nil && resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusServiceUnavailable {
				atomic.AddInt64(&r.Suite.PutErr, 1)
				atomic.AddInt64(&r.Suite.PutCount, -1)
			} else {
				fmt.Printf("Upload status %s: resp: %+v\n", resp.Status, resp)
				if resp.Body != nil {
					body, _ := ioutil.ReadAll(resp.Body)
					fmt.Printf("Body: %s\n", string(body))
				}
			}
		}
	}
}

func (r *BenchmarkSteps) RunGet(delay time.Duration) {
	time.Sleep(delay)
	defer r.MarkDuration(time.Now(), &r.GetMillis)

	cli := r.Suite.CreateS3Client()
	counter := 0
	end := time.Now().Add(time.Duration(r.Config.DurationSeconds) * time.Second)

	for time.Now().Before(end) {
		if len(r.Objects) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		atomic.AddInt64(&r.Suite.GetCount, 1)

		//pos := rand.Int() % len(r.Objects) // if want random
		pos := counter % len(r.Objects)
		counter++
		if counter < 0 {
			counter = 0
		}

		objName := r.Suite.CreateUrl(r.Objects[pos])
		req, _ := http.NewRequest("GET", objName, nil)
		if resp, err := cli.Hit(req); err != nil {
			log.Fatalf("FATAL: Error downloading object %s: %v", objName, err)
		} else if resp != nil && resp.Body != nil {
			if resp.StatusCode == http.StatusServiceUnavailable {
				atomic.AddInt64(&r.Suite.GetErr, 1)
				atomic.AddInt64(&r.Suite.GetCount, -1)
			} else {
				_, _ = io.Copy(ioutil.Discard, resp.Body)
			}
		}
	}
}

func (r *BenchmarkSteps) RunList(delay time.Duration) {
	time.Sleep(delay)
	defer r.MarkDuration(time.Now(), &r.ListMillis)

	cli := r.Suite.CreateS3Client()
	end := time.Now().Add(time.Duration(r.Config.DurationSeconds) * time.Second)
	counter := 0
	var continuationToken *string
	var prefix string

	newPrefix := func() {
		l := len(r.Objects)
		if l <= 0 {
			time.Sleep(10 * time.Millisecond)
			return
		}

		pos := counter % l
		counter++
		if counter < 0 {
			counter = 0
		}
		prefix = veeamPrefix
		switch counter % 4 {
		case 0:
			prefix += S.LeftOf(r.Objects[pos], `/`)
		case 1:
			prefix += S.LeftOfLast(r.Objects[pos], `/`)
		case 2:
			suffix := S.LeftOfLast(S.LeftOfLast(r.Objects[pos], `/`), `/`)
			prefix += suffix
		}
	}
	newPrefix()

	for time.Now().Before(end) {
		atomic.AddInt64(&r.Suite.ListCount, 1)

		//pos := rand.Int() % len(r.Objects) // if want random

		in := &s3.ListObjectsV2Input{
			Bucket:            aws.String(r.Config.BucketName),
			MaxKeys:           aws.Int64(1000),
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
			Delimiter:         aws.String(`/`),
		}
		res, err := cli.ListObjectsV2(in)
		if err != nil {
			atomic.AddInt64(&r.Suite.ListErr, 1)
			atomic.AddInt64(&r.Suite.ListCount, -1)
			log.Printf(`WARNING: failed %v %s`, in, err)
		}
		if res != nil {
			total := int64(len(res.Contents) + len(res.CommonPrefixes))
			atomic.AddInt64(&r.Suite.ListRowsCount, total)
		}
		if res == nil || len(res.Contents) == 0 || res.NextContinuationToken == nil {
			newPrefix()
		} else {
			continuationToken = res.NextContinuationToken
		}
	}
}

func (r *BenchmarkSteps) RunDel(delay time.Duration) {
	time.Sleep(delay)
	defer r.MarkDuration(time.Now(), &r.DelMillis)

	cli := r.Suite.CreateS3Client()
	counter := 0

	end := time.Now().Add(time.Duration(r.Config.DurationSeconds) * time.Second)

	for time.Now().Before(end) {
		if counter >= len(r.Objects) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		objName := r.Suite.CreateUrl(r.Objects[counter])
		counter++
		req, _ := http.NewRequest("DELETE", objName, nil)
		if resp, err := cli.Hit(req); err != nil {
			log.Fatalf("FATAL: Error deleting object %s: %v", objName, err)
		} else if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
			atomic.AddInt64(&r.Suite.DelCount, -1)
			atomic.AddInt64(&r.Suite.DelErr, -1)
		} else {
			atomic.AddInt64(&r.Suite.DelCount, 1)
		}
	}
}

func (r *BenchmarkSteps) AppendObjects() {
	newObjects := r.PutSeed.NextVeeamFiles(r.Config.MaxFolder1Capacity, r.Config.MaxFolder2Capacity, r.Config.MaxFolder3Capacity)
	r.Objects = append(r.Objects, newObjects...)
}

func (r *BenchmarkSteps) MarkDuration(start time.Time, v *uint64) {
	atomic.AddUint64(v, uint64(time.Now().Sub(start).Milliseconds()))
	defer r.WaitGroup.Done()
}

////////////////////////////////////////////////////////////////////////////////
// main

func main() {
	// parse benchmark parameter
	b := BenchConfig{}
	b.SetDefaults()
	errStr, exitCode := b.ParseFromArgs(os.Args[1:])
	if exitCode != 0 {
		fmt.Println(exitCode, errStr)
		os.Exit(exitCode)
	}

	// run benchmark
	bs := BenchmarkSuite{}
	bs.FromConfig(&b).Run()
}
