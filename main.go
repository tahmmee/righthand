package main

import (
	"flag"
	"github.com/couchbase/go-couchbase"
	"log"
)

// endpoint connection
type EndPoint struct {
	Host       string
	BucketName string
}

func NewEndPoint(host, bucket string) *EndPoint {
	return &EndPoint{
		Host:       host,
		BucketName: bucket,
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
	hostName := flag.String("host", "http://127.0.0.1:8091", "host:port to connect to")
	bucketName := flag.String("bucket", "default", "bucket for data loading")
	loaders := flag.Int("stress", 5, "number of concurrent data loaders")
	flag.Parse()

	endPoint := NewEndPoint(*hostName, *bucketName)
	streamer := NewStreamManager(endPoint)

	for i := 0; i < *loaders; i++ {
		go streamer.GenerateMutations()
	}

	streamer.VerifyVBucket(0)

}
