package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
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

type Mutation struct {
	Key   string
	SeqNo uint64
	Value map[string]interface{}
}
type StreamMutations struct {
	Docs    []Mutation
	VBucket uint16
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
	loaders := flag.Int("stress", 5, "number of concurrent data loaders")
	flag.Parse()

	endPoint := EndPoint{
		Host:   *hostName,
		Bucket: *bucketName,
	}

	for i := 0; i < *loaders; i++ {
		go BgLoader(endPoint)
	}

	bucket := endPoint.Connect()
	defer bucket.Close()

	// start rollback detection
	var startSequence uint64
	var endSequence uint64 = 0xFFFFFFFF
	var highSequenceStat string
	var lastStreamMutations StreamMutations
	var vb uint16 = 955

	// get current high seqno
	highSequenceStat = GetVBucketStat(endPoint, vb, "high_seqno")
	startSequence, _ = strconv.ParseUint(highSequenceStat, 10, 64)

	// get current vb uuid
	uuid := GetVBucketStat(endPoint, vb, "uuid")
	lastUuid := uuid

	feed, err := bucket.StartUprFeed("rth-integrity", 0xFFFF)
	logerr(err)

	for {

		if lastUuid != uuid { /* vbucket takeover */
			fmt.Println("VBUCKET Takeover", lastUuid, uuid)
			// verify data from last stream
			VerifyLastStreamMutations(endPoint, lastStreamMutations, startSequence, endSequence)
			lastUuid = uuid
		}

		// let mutations run
		time.Sleep(time.Second * time.Duration((*mutationWindow)))

		// get new high seqno
		highSequenceStat = GetVBucketStat(endPoint, vb, "high_seqno")
		endSequence, _ = strconv.ParseUint(highSequenceStat, 10, 64)
		uuid64, _ := strconv.ParseUint(uuid, 10, 64)

		// stream mutations
		lastStreamMutations, err = streamVB(feed, vb, uuid64, startSequence, endSequence)

		if err != nil {
			// error
			fmt.Println("Error during stream request: %s ...reconnecting", err)
			// create a new stream
			feed, err = bucket.StartUprFeed("rth-integrity", 0xFFFF)
		}

		// set startSequence to endSequence
		startSequence = endSequence

		// update uuid
		uuid = GetVBucketStat(endPoint, vb, "uuid")
	}

}

func BgLoader(endPoint EndPoint) {
	var i uint64 = 0
	bucket := endPoint.Connect()
	for {
		key := fmt.Sprintf("%s-rth_%d", RandStr(12), i)
		doc := make(map[string]interface{})
		doc["is_rthloader"] = true
		doc["id"] = key
		doc["value"] = i
		if err := bucket.Set(key, 0, doc); err != nil {
			fmt.Println("Set err: %s ...reconnecting", err)
			bucket = endPoint.Connect()
		}
		i++
	}
}

func streamVB(feed *couchbase.UprFeed, vb uint16, vbuuid, startSequence, endSequence uint64) (StreamMutations, error) {

	fmt.Println("STREAM: ", vb, vbuuid, startSequence, endSequence)

	mutations := StreamMutations{
		Docs:    []Mutation{},
		VBucket: vb,
	}

	err := feed.UprRequestStream(vb, 0, 0, vbuuid, startSequence, endSequence, startSequence, startSequence)
	if err != nil {
		//panic(err)
		return mutations, err
	}
	// close stream
	defer feed.UprCloseStream(vb, 0)

	// stream all mutations
	item := <-feed.C
	opcode := item.Opcode
	if log := item.FailoverLog; log != nil {
		fmt.Println((*log)[0])
	}
	if opcode != gomemcached.UPR_STREAMREQ {
		emsg := fmt.Sprintf("Invalid stream request: ", opcode)
		return mutations, errors.New(emsg)
	}

	for dcpItem := range feed.C {
		if dcpItem.Opcode == gomemcached.UPR_STREAMEND {
			break
		}

		if dcpItem.Opcode == gomemcached.UPR_MUTATION {
			key := fmt.Sprintf("%s", dcpItem.Key)
			var seqNo uint64 = dcpItem.Seqno
			value := make(map[string]interface{})
			if ok := json.Unmarshal(dcpItem.Value, &value); ok == nil {
				if _, exist := value["is_rthloader"]; exist {
					m := Mutation{
						Key:   key,
						SeqNo: seqNo,
						Value: value,
					}
					mutations.Docs = append(mutations.Docs, m)
				}
			}
		}
	}
	return mutations, nil
}

func VerifyLastStreamMutations(endPoint EndPoint, mutations StreamMutations, startSequence, endSequence uint64) {
	fmt.Println(startSequence, endSequence)

	// get new high seqNo
	highSequenceStat := GetVBucketStat(endPoint, mutations.VBucket, "high_seqno")
	// newHighSequence, _ := strconv.ParseUint(highSequenceStat, 10, 64)
	panic("VERIFY TAKEVOER! " + highSequenceStat)
}

func GetVBucketStat(endPoint EndPoint, vb uint16, key string) string {
	bucket := endPoint.Connect()
	defer bucket.Close()

	var highSequence string
	statKey := fmt.Sprintf("vb_%d:%s", vb, key)
	stats := bucket.GetStats("vbucket-details")
	for _, vbStats := range stats {
		for stat, value := range vbStats {
			if stat == statKey {
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

/* failover log entry:
[[251713123684111 1173534] [116833564910019 1085339]
	* means that the latest vbuiid took over at 1173534
	* which means that many mutations on last vbuuid are gone
*/
// keep concurrent map of key:seqno across snapshots
// whenever a stream ends - restart
// on stream start check to see last known seqno of vb
// if first seqno is < last seqno then some other clients did a rollback
// 		verification: make sure rolledback keys are missing from mc, view, n1ql
