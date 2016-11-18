package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"log"
	"regexp"
	"strconv"
	"time"
)

// endpoint connection
type EndPoint struct {
	Host   string
	Bucket string
}

func (e *EndPoint) Connect() *couchbase.Bucket {

	c, err := couchbase.Connect(e.Host)
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := c.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	bucket, err := pool.GetBucket(e.Bucket)
	if err != nil {
		log.Fatalf("Error getting bucket:  %v", err)
	}

	return bucket
}

func main() {
	hostName := flag.String("host", "http://127.0.0.1:8091", "host:port to connect to")
	bucketName := flag.String("bucket", "default", "bucket for data loading")
	mutationWindow := flag.Int("window", 20, "time between stream sampling")
	flag.Parse()

	endPoint := EndPoint{
		Host:   *hostName,
		Bucket: *bucketName,
	}
	bucket := endPoint.Connect()

	// start rollback detection
	var startSequence uint64
	var endSequence uint64 = 0xFFFFFFFF
	var highSequenceStat string
	var lastMutationKey string

	// get current high seqno and vb uuid
	uuid := GetVBucketStatHighSequence(bucket, "vb_1000:uuid")
	highSequenceStat = GetVBucketStatHighSequence(bucket, "vb_1000:high_seqno")
	startSequence, _ = strconv.ParseUint(highSequenceStat, 10, 64)
	for {

		if startSequence > endSequence {
			// rollback detected
			// should not expect lastMutationKey to be retrievable
			// this does require that we have seqno up to lastMutationKey
			panic("VERIFY ROLLBACK!")
		}

		// let mutations run
		time.Sleep(time.Second * time.Duration((*mutationWindow)))

		// get new high seqno
		highSequenceStat = GetVBucketStatHighSequence(bucket, "vb_1000:high_seqno")
		endSequence, _ = strconv.ParseUint(highSequenceStat, 10, 64)
		uuid64, _ := strconv.ParseUint(uuid, 10, 64)

		// stream mutations
		fmt.Println("STREAM", uuid64, startSequence, endSequence)
		codeCh := streamVB(bucket, 1000, uuid64, startSequence, endSequence)

		lastMutationKey = <-codeCh
		fmt.Println(lastMutationKey)

		// set startSequence to endSequence
		startSequence = endSequence
	}

}

func streamVB(bucket *couchbase.Bucket, vb uint16, vbuuid, startSequence, endSequence uint64) chan string {

	ch := make(chan string)

	// prepare dcp stream
	var seq uint32 = 0xFFFF
	feed, err := bucket.StartUprFeed("rth-integrity", seq)
	if err != nil {
		// TODO+ RETRY
		// log.Fatalf("Error create dcp feed:  %v", err)
	}

	//vb, flags, opaque, vuuid, startSequence, endSequence, snapStart, snapEnd uint64)
	err = feed.UprRequestStream(vb, 0, 0, vbuuid, startSequence, endSequence, startSequence, startSequence)
	if err != nil {
		log.Fatalf("Error starting dcp stream:  %v", err)
	}

	go func() {

		// stream all mutations
		item := <-feed.C
		opcode := item.Opcode
		var lastKey string
		if opcode == gomemcached.UPR_STREAMREQ {
			for dcpItem := range feed.C {
				if dcpItem.Opcode == gomemcached.UPR_STREAMEND {
					break
				}
				if dcpItem.Opcode == gomemcached.UPR_MUTATION {
					key := fmt.Sprintf("%s", dcpItem.Key)
					seqNo := dcpItem.Seqno
					value := make(map[string]interface{})
					if ok := json.Unmarshal(dcpItem.Value, &value); ok == nil {
						fmt.Println(key, seqNo)
					}
					lastKey = key
				}
			}
		} else {
			panic("REQ FAILED!")
		}

		// close stream
		feed.Close()
		ch <- lastKey
	}()

	return ch
}

func streamData(bucket *couchbase.Bucket) {

	// prepare dcp stream
	var seq uint32 = 0xFFFF
	feed, err := bucket.StartUprFeed("rth-integrity", seq)
	if err != nil {
		log.Fatalf("Error create dcp feed:  %v", err)
	}

	//vb, flags, opaque, vuuid, startSequence, endSequence, snapStart, snapEnd uint64)
	err = feed.UprRequestStream(1000, 0, 0, 0, 0, 0xFFFFFFFF, 0, 0)
	if err != nil {
		log.Fatalf("Error starting dcp stream:  %v", err)
	}

	for dcpItem := range feed.C {
		fmt.Println(dcpItem.Opcode)
		if dcpItem.Opcode == gomemcached.UPR_STREAMEND {
			streamData(bucket)
		}
		if dcpItem.Opcode == gomemcached.UPR_MUTATION {
			key := fmt.Sprintf("%s", dcpItem.Key)
			seqNo := dcpItem.Seqno
			value := make(map[string]interface{})
			if ok := json.Unmarshal(dcpItem.Value, &value); ok == nil {
				fmt.Println(key, seqNo)
			}
		}
	}

}

func GetVBucketStatHighSequence(bucket *couchbase.Bucket, key string) string {
	var highSequence string
	stats := bucket.GetStats("vbucket-details")
	for _, vbStats := range stats {
		for stat, value := range vbStats {
			if stat == key {
				highSequence = value
				return highSequence
			}
		}
	}
	return highSequence
}

func RandStr(size int) string {
	rb := make([]byte, size)
	_, err := rand.Read(rb)
	logerr(err)
	str := base64.URLEncoding.EncodeToString(rb)
	reg, err := regexp.Compile("[^A-Za-z0-9]+")
	logerr(err)
	return reg.ReplaceAllString(str, "")
}

func logerr(err error) {
	if err != nil {
		log.Fatalf("Error occurred:  %v", err)
	}
}

// keep concurrent map of key:seqno across snapshots
// whenever a stream ends - restart
// on stream start check to see last known seqno of vb
// if first seqno is < last seqno then some other clients did a rollback
// 		verification: make sure rolledback keys are missing from mc, view, n1ql
