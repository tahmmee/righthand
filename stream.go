package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"strconv"
	"time"
)

type StreamManager struct {
	endPoint *EndPoint
}

func NewStreamManager(endPoint *EndPoint) *StreamManager {
	return &StreamManager{endPoint: endPoint}
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

func (s *StreamManager) GenerateMutations() {
	var i uint64 = 0
	bucket := s.endPoint.Bucket()
	for {
		key := fmt.Sprintf("%s-rth_%d", RandStr(12), i)
		doc := make(map[string]interface{})
		doc["is_rthloader"] = true
		doc["id"] = key
		doc["value"] = i
		if err := bucket.Set(key, 0, doc); err != nil {
			bucket.Close()
			bucket = s.endPoint.Bucket()
		}
		i++
	}

	bucket.Close()
}

func (s *StreamManager) VerifyVBucket(vb uint16) int {

	bucket := s.endPoint.Bucket()
	stats := NewVBucketStats(s.endPoint)

	// start rollback detection
	var startSequence uint64
	var endSequence uint64 = 0xFFFFFFFF
	var highSequenceStat string
	var lastStreamMutations StreamMutations

	// get current vb uuid
	uuid := stats.UUID(vb)

	// start upr feed
	feed, err := bucket.StartUprFeed("rth-integrity", 0xFFFF)
	logerr(err)

	// get current high seqno as stream start
	highSequenceStat = stats.HighSequence(vb)
	startSequence, _ = strconv.ParseUint(highSequenceStat, 10, 64)

	uuid64, _ := strconv.ParseUint(uuid, 10, 64)

	// stream mutations
	streamCh, err := s.StreamMutations(feed,
		vb,
		uuid64,
		startSequence,
		0xFFFFFFFF)

	if err != nil {
		// error
		fmt.Println("Error during stream request: %s", err)
		return ERR_STREAM_FAILED
	}

	lastStreamMutations = <-streamCh

	// detect if uuid changed (ie.. vbucket takeover)
	didVBTakover := false
	for i := 0; i < 5; i++ {
		didVBTakover = (stats.UUID(vb) != uuid)

		if didVBTakover {
			break // takeover detected
		}

		// retry wait
		time.Sleep(time.Second * 5)
		fmt.Println("Expected change in uuid... retry", uuid)
	}

	if !didVBTakover {
		// did not verify a takeover phase
		fmt.Println("Stream ended without a takeover occurring...exiting")
		return ERR_NO_VB_TAKEOVER
	}

	// verify takeover
	return s.VerifyLastStreamMutations(lastStreamMutations, startSequence, endSequence)
}

func (s *StreamManager) StreamMutations(feed *couchbase.UprFeed, vb uint16, vbuuid, startSequence, endSequence uint64) (chan StreamMutations, error) {

	mutationsCh := make(chan StreamMutations)
	mutations := NewStreamMutations(vb)

	err := feed.UprRequestStream(vb, 0, 0, vbuuid, startSequence, endSequence, startSequence, startSequence)
	if err != nil {
		return mutationsCh, err
	} else {
		fmt.Println("Streaming: ", vb, vbuuid, startSequence, endSequence)
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
		cachedMutations := []Mutation{}
		for {
			select {
			case dcpItem := <-feed.C:
				if dcpItem.Opcode == gomemcached.UPR_STREAMEND {

					// prepend with cached mutations
					mutations.Docs = append(cachedMutations, mutations.Docs[0:]...)
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
								cachedMutations = mutations.Docs
								mutations.Docs = []Mutation{m}
							} else {
								mutations.Docs = append(mutations.Docs, m)
							}
						}
					}
				}
			case <-time.After(time.Second * 5):
				// no item in 5 seconds then give up
				mutations.Docs = append(cachedMutations, mutations.Docs[0:]...)
				mutationsCh <- mutations
				return
			}
		}
	}()
	return mutationsCh, nil
}

func (s *StreamManager) VerifyLastStreamMutations(mutations StreamMutations, startSequence, endSequence uint64) int {

	vb := mutations.VBucket
	stats := NewVBucketStats(s.endPoint)

	// get takeover seqno from new failover log
	takeoverSequenceStat := stats.FailoverSequence(vb, "0")
	takeoverSequence, _ := strconv.ParseUint(takeoverSequenceStat, 10, 64)
	fmt.Println("Verify takover occurred at sequence: ", takeoverSequence)

	// get all keys higher than takeover seqno which should be rolled back
	rollbackDocs := []Mutation{}
	for _, m := range mutations.Docs {
		fmt.Println(m.SeqNo, m.Key)
		if m.SeqNo > takeoverSequence {
			rollbackDocs = append(rollbackDocs, m)
			fmt.Println("Rollback: ", m.Key)
		}
	}

	return ROLLBACK_OK
	// all of these keys should be missing
	// verify kv
	// verify views
	// verify index
	// keys prior to takeover seqno should be present

	// get new high seqNo
	// uuid := GetVBucketStat(endPoint, mutations.VBucket, "uuid")
	// panic("VERIFY TAKEVOER! " + uuid + " : " + takeoverSequenceStat)
}
