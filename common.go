package main

import (
	"crypto/rand"
	"encoding/base64"
	"log"
	"regexp"
)

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
