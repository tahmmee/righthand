package main

import (
	"flag"
	"github.com/couchbase/go-couchbase"
	"log"
	"os"
)

// endpoint connection
type EndPoint struct {
	Host       string
	BucketName string
	QueryHost  string
}

func NewEndPoint(host, queryHost, bucket string) *EndPoint {
	return &EndPoint{
		Host:       host,
		BucketName: bucket,
		QueryHost:  queryHost,
	}
}

func (e *EndPoint) Bucket() *couchbase.Bucket {

	c, err := couchbase.Connect(e.Host)
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := c.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	bucket, err := pool.GetBucket(e.BucketName)
	if err != nil {
		log.Fatalf("Error getting bucket:  %v", err)
	}

	return bucket
}

func main() {
	hostName := flag.String("host", "http://127.0.0.1:8091", "data host:port to connect to")
	queryHost := flag.String("query", "http://127.0.0.1:8093", "query host:port to connect to")
	bucketName := flag.String("bucket", "default", "bucket for data loading")
	stress := flag.Int("stress", 5, "number of concurrent data loaders")
	vb := flag.Uint("vbucket", 0, "vbucket to verify")
	flag.Parse()

	endPoint := NewEndPoint(*hostName, *queryHost, *bucketName)
	streamer := NewStreamManager(endPoint)
	if err := streamer.CreateIndexes(); err != nil {
		os.Exit(ERR_INDEX_CREATE)
	}

	for i := 0; i < *stress; i++ {
		go streamer.GenerateMutations()
	}

	rc := streamer.VerifyVBucket(uint16(*vb))
	os.Exit(rc)
}
