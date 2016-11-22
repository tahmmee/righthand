package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type StreamManager struct {
	endPoint    *EndPoint
	StreamEnded bool
}

func NewStreamManager(endPoint *EndPoint) *StreamManager {
	return &StreamManager{
		endPoint:    endPoint,
		StreamEnded: false,
	}
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
	for s.StreamEnded == false {
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

func (s *StreamManager) CreateIndexes() error {

	// view
	bucket := s.endPoint.Bucket()
	ddoc := `{"views":{"values":{"map":"function(doc, meta){ if(doc.is_rthloader){  emit(meta.id, doc.value); } }"}}} `
	if err := bucket.PutDDoc("integrity", ddoc); err != nil {
		fmt.Println("unable to create view", err)
		return err
	}

	// 2i
	uri := fmt.Sprintf("%s/query/service", s.endPoint.QueryHost)
	params := url.Values{"statement": {"create index rth_id on `default`(id)"}}
	if _, err := http.PostForm(uri, params); err != nil {
		fmt.Println("unable to create index", err)
		return err
	}

	return nil
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

	s.StreamEnded = true

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

func (s *StreamManager) VerifyViewDocs(docs []Mutation, shouldExist bool) bool {
	bucket := s.endPoint.Bucket()

	for _, doc := range docs {
		viewParams := map[string]interface{}{
			"stale": false,
			"key":   doc.Key,
		}

		res, err := bucket.View("integrity", "values", viewParams)
		if err != nil {
			fmt.Println("Error querying View:  %v", err)
			return false
		}
		row := res.Rows[0]
		fmt.Println(row)
		//viewSum := row.Value.(float64)
		//expectedSum := float64(sum)
	}

	return true
}

func (s *StreamManager) VerifyKVDocs(docs []Mutation, shouldExist bool) bool {
	bucket := s.endPoint.Bucket()
	rdoc := make(map[string]interface{})

	// get doc from mcd engine
	for _, doc := range docs {
		if err := bucket.Get(doc.Key, &rdoc); err != nil {
			// doc exists
			if shouldExist == false {
				// but doc should not exist
				fmt.Println("ERROR [kv]: expected rollback doc to be deleted", doc.Key)
				return false
			}
		} else {
			// doc is missing
			if shouldExist == true {
				// but doc should exist
				fmt.Println("ERROR [kv]: expected doc to exist after rollback", doc.Key)
				return false
			}
		}
	}

	return true
}
func (s *StreamManager) VerifyQueryDocs(docs []Mutation, shouldExist bool) bool {

	uri := fmt.Sprintf("%s/query/service", s.endPoint.QueryHost)
	for _, doc := range docs {

		// select key
		sel := fmt.Sprintf("select * from  `default` where id=='%s'", doc.Key)
		params := url.Values{"statement": {sel}}
		res, err := http.PostForm(uri, params)
		if err != nil || res.StatusCode != 200 {
			fmt.Println("failed to query index", err)
			logerr(err)
		}

		// read response
		body, err := ioutil.ReadAll(res.Body)
		logerr(err)
		var v map[string]interface{}
		err = json.Unmarshal(body, &v)
		logerr(err)

		// expect resultCount = 0
		metrics := v["metrics"].(map[string]interface{})
		nDocs := metrics["resultCount"].(float64)
		fmt.Println("Query resultCount shouldExist?", nDocs, shouldExist)
		if shouldExist && nDocs == 0 {
			fmt.Printf("ERROR [2i]: expected key (%s | %d) to exist after rollback \n", doc.Key, doc.SeqNo)
			return false
		}
		if shouldExist == false && nDocs > 0 {
			fmt.Printf("ERROR [2i]: expected key (%s | %d) to be removed after rollback\n", doc.Key, doc.SeqNo)
			return false
		}
	}

	return true
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
	persistedDocs := []Mutation{}
	for _, m := range mutations.Docs {
		fmt.Println(m.SeqNo, m.Key)
		if m.SeqNo > takeoverSequence {
			rollbackDocs = append(rollbackDocs, m)
		} else {
			persistedDocs = append(persistedDocs, m)
		}
	}

	if len(rollbackDocs) == 0 {
		fmt.Println("dcp rollback did not occur after takeover")
		return ERR_NO_ROLLBACK
	}

	return s.VerifyEngines(rollbackDocs, persistedDocs)

}

func (s *StreamManager) VerifyEngines(rollbackDocs, persistedDocs []Mutation) int {

	// view expect rollback docs to be missing
	if ok := s.VerifyViewDocs(rollbackDocs, false); ok == false {
		return ERR_ROLLBACK_FAIL
	}

	// view expect persisted docs to be present
	if ok := s.VerifyViewDocs(persistedDocs, true); ok == false {
		return ERR_ROLLBACK_FAIL
	}

	// 2i expect rollback docs to be missing
	if ok := s.VerifyQueryDocs(rollbackDocs, false); ok == false {
		return ERR_ROLLBACK_FAIL
	}

	// 2i expect persisted docs to be present
	if ok := s.VerifyQueryDocs(persistedDocs, true); ok == false {
		return ERR_ROLLBACK_FAIL
	}

	// kv expect rollback docs to be missing
	if ok := s.VerifyKVDocs(rollbackDocs, false); ok == false {
		return ERR_ROLLBACK_FAIL
	}

	// kv expect persisted docs to be present
	if ok := s.VerifyKVDocs(persistedDocs, true); ok == false {
		return ERR_ROLLBACK_FAIL
	}

	return ROLLBACK_OK
}
