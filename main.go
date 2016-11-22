package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"log"
	"strconv"
	"time"
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

type Mutation struct {
	Key   string
	SeqNo uint64
	Value map[string]interface{}
}
type StreamMutations struct {
	Docs    []Mutation
	VBucket uint16
}

func NewStreamMutations(vb uint16) StreamMutations {
	return StreamMutations{
		Docs:    []Mutation{},
		VBucket: vb,
	}
}
func main() {
	hostName := flag.String("host", "http://127.0.0.1:8091", "host:port to connect to")
	bucketName := flag.String("bucket", "default", "bucket for data loading")
	loaders := flag.Int("stress", 5, "number of concurrent data loaders")
	flag.Parse()

	endPoint := NewEndPoint(*hostName, *bucketName)
	stats := NewVBucketStats(endPoint)

	for i := 0; i < *loaders; i++ {
		go BgLoader(endPoint)
	}

	bucket := endPoint.Bucket()
	defer bucket.Close()

	// start rollback detection
	var startSequence uint64
	var endSequence uint64 = 0xFFFFFFFF
	var highSequenceStat string
	var lastStreamMutations StreamMutations
	var vb uint16 = 0

	// get current vb uuid
	uuid := stats.UUID(vb)

	// start upr feed
	feed, err := bucket.StartUprFeed("rth-integrity", 0xFFFF)
	logerr(err)

	for {

		// get current high seqno as stream start
		highSequenceStat = stats.HighSequence(vb)
		startSequence, _ = strconv.ParseUint(highSequenceStat, 10, 64)

		uuid64, _ := strconv.ParseUint(uuid, 10, 64)

		// stream mutations
		streamCh, err := streamVB(feed, vb, uuid64, startSequence, 0xFFFFFFFF)

		if err != nil {
			// error
			fmt.Println("Error during stream request: %s ...reconnecting", err)
			feed, err = bucket.StartUprFeed("rth-integrity", 0xFFFF)
		} else {

			streamMutations := <-streamCh
			if len(streamMutations.Docs) > 0 {
				lastStreamMutations = streamMutations
			}
		}
		if len(lastStreamMutations.Docs) > 0 {
			m := lastStreamMutations.Docs[len(lastStreamMutations.Docs)-1]
			fmt.Println(m.Key, m.SeqNo)
		}

		// update uuid
		newUuid := stats.UUID(vb)
		if newUuid != uuid { /* vbucket takeover */
			fmt.Println("VBUCKET Takeover", newUuid, uuid)
			// verify data from last stream
			VerifyLastStreamMutations(endPoint, lastStreamMutations, startSequence, endSequence)
			uuid = newUuid
		}
	}

}

func BgLoader(endPoint *EndPoint) {
	var i uint64 = 0
	bucket := endPoint.Bucket()
	for {
		key := fmt.Sprintf("%s-rth_%d", RandStr(12), i)
		doc := make(map[string]interface{})
		doc["is_rthloader"] = true
		doc["id"] = key
		doc["value"] = i
		if err := bucket.Set(key, 0, doc); err != nil {
			bucket.Close()
			bucket = endPoint.Bucket()
		}
		i++
	}

	bucket.Close()
}

func streamVB(feed *couchbase.UprFeed, vb uint16, vbuuid, startSequence, endSequence uint64) (chan StreamMutations, error) {

	fmt.Println("STREAM: ", vb, vbuuid, startSequence, endSequence)

	mutationsCh := make(chan StreamMutations)
	mutations := NewStreamMutations(vb)

	err := feed.UprRequestStream(vb, 0, 0, vbuuid, startSequence, endSequence, startSequence, startSequence)
	if err != nil {
		return mutationsCh, err
	}

	// stream all mutations
	item := <-feed.C
	opcode := item.Opcode
	if log := item.FailoverLog; log != nil {
		fmt.Println((*log)[0])
	}
	if opcode != gomemcached.UPR_STREAMREQ {
		emsg := fmt.Sprintf("Invalid stream request: ", opcode)
		return mutationsCh, errors.New(emsg)
	}

	go func() {

		// close stream whenever streaming is done
		defer feed.UprCloseStream(vb, 0)

		maxMutations := 100
		for {
			select {
			case dcpItem := <-feed.C:
				if dcpItem.Opcode == gomemcached.UPR_STREAMEND {
					mutationsCh <- mutations
					return
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
							if len(mutations.Docs) > maxMutations {
								mutations.Docs = []Mutation{m}
							} else {
								mutations.Docs = append(mutations.Docs, m)
							}
						}
					}
				}
			case <-time.After(time.Second * 5):
				// no item in 5 seconds then give up
				fmt.Println("TIMEOUT!")
				mutationsCh <- mutations
				return
			}
		}
	}()
	return mutationsCh, nil
}

func VerifyLastStreamMutations(endPoint *EndPoint, mutations StreamMutations, startSequence, endSequence uint64) {

	vb := mutations.VBucket
	statHelper := NewVBucketStats(endPoint)

	// get takeover seqno from new failover log
	takeoverSequenceStat := statHelper.FailoverSequence(vb, "0")
	takeoverSequence, _ := strconv.ParseUint(takeoverSequenceStat, 10, 64)

	fmt.Println("TAKEVOER", takeoverSequence, "START", startSequence, "END", endSequence)
	// get all keys higher than takeover seqno which should be rolled back
	rollbackDocs := []Mutation{}
	for _, m := range mutations.Docs {
		fmt.Println(m.SeqNo, m.Key)
		if m.SeqNo > takeoverSequence {
			rollbackDocs = append(rollbackDocs, m)
			fmt.Println("Rollback: ", m.Key)
		}
	}
	// all of these keys should be missing
	// verify kv
	// verify views
	// verify index
	// keys prior to takeover seqno should be present

	// get new high seqNo
	// uuid := GetVBucketStat(endPoint, mutations.VBucket, "uuid")
	// panic("VERIFY TAKEVOER! " + uuid + " : " + takeoverSequenceStat)
}
