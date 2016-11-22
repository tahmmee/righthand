package main

import (
	"crypto/rand"
	"encoding/base64"
	"log"
	"regexp"
)

const (
	ROLLBACK_OK        = iota
	ERR_ROLLBACK_FAIL  = iota
	ERR_NO_ROLLBACK    = iota
	ERR_STREAM_FAILED  = iota
	ERR_NO_VB_TAKEOVER = iota
	ERR_INDEX_CREATE   = iota
	ERR_VIEW_QUERY     = iota
	ERR_QUERY_FAILED   = iota
)

type Rest struct {
	Host string
}

//func (r *Rest) Query(staement string) err{
//}

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
