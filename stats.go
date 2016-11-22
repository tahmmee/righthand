package main

import (
	"fmt"
	"github.com/couchbase/go-couchbase"
)

type VBucketStats struct {
	Bucket *couchbase.Bucket
}

func NewVBucketStats(endPoint *EndPoint) *VBucketStats {
	bucket := endPoint.Bucket()
	return &VBucketStats{Bucket: bucket}
}

func (s *VBucketStats) FailoverSequence(vb uint16, entry string) string {

	var failoverSequence string
	statKey := fmt.Sprintf("vb_%d:%s:seq", vb, entry)
	stats := s.Bucket.GetStats("failovers")
	for _, vbStats := range stats {
		failoverSequence = vbStats[statKey]
		break
	}

	return failoverSequence
}
func (s *VBucketStats) UUID(vb uint16) string {
	return s.GetVVBucketStat(vb, "uuid")
}
func (s *VBucketStats) HighSequence(vb uint16) string {
	return s.GetVVBucketStat(vb, "high_seqno")
}

func (s *VBucketStats) GetVVBucketStat(vb uint16, key string) string {

	var highSequence string
	statKey := fmt.Sprintf("vb_%d:%s", vb, key)
	stats := s.Bucket.GetStats("vbucket-details")
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
