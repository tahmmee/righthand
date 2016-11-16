package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"log"
	"regexp"
)

func connectToBucket(host, name string) *couchbase.Bucket {

	c, err := couchbase.Connect(host)
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := c.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	bucket, err := pool.GetBucket(name)
	if err != nil {
		log.Fatalf("Error getting bucket:  %v", err)
	}

	return bucket
}

func main() {
	bucket := connectToBucket("http://127.0.0.1:9000/", "default")
	go streamData()

	loop := 0
	for {
		sum := 0
		ndocs := 1000
		for i := 0; i < ndocs; i++ {
			key := fmt.Sprintf("loop_%d-%s", loop, RandStr(12))
			doc := make(map[string]interface{})
			doc["loader"] = "righthand"
			doc["id"] = fmt.Sprintf("rthkey-%d", i)
			doc["value"] = i
			bucket.Set(key, 0, doc)
			sum += i
		}

		// view verification
		viewParams := map[string]interface{}{
			"stale": false,
		}
		res, err := bucket.View("integrity", "values", viewParams)
		if err != nil {
			log.Fatalf("Error querying View:  %v", err)
		}
		row := res.Rows[0]
		viewSum := row.Value.(float64)
		expectedSum := float64(sum)
		if expectedSum != viewSum {
			panic("View Sum Should Match KV Sum")
		}

		loop++
	}
}

func streamData() {
	bucket := connectToBucket("http://127.0.0.1:9000/", "default")

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
			streamData()
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
