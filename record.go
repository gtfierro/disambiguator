package main

import (
	"bytes"
	_ "fmt"
)

var records = [](*Record){}
var recordChannel = make(chan *Record)

type Record struct {
	raw          string
	clean        string
	actual       string
	collectionID int
	Id           string `json:"id,omitempty"`
}

func (r *Record) getRawSimilarity(s *Record) float64 {
	return jaro([]byte(r.clean), []byte(s.clean))
}

func makeRecord(raw []byte) *Record {
	raw = bytes.ToUpper(raw)
	raw = bytes.Trim(raw, "\n")
	rawclean := clean(raw) //TODO: remove common terms from clean
	r := &Record{raw: string(raw), clean: string(rawclean)}
	return r
}

func convertInterfaceToRecord(i interface{}) *Record {
	r := i.(map[string]interface{})
	var rec *Record
	if r["actual"] == nil {
		rec = &Record{raw: r["raw"].(string), clean: r["clean"].(string), collectionID: int(r["collectionID"].(float64))}
	} else {
		rec = &Record{raw: r["raw"].(string), clean: r["clean"].(string), actual: r["actual"].(string), collectionID: int(r["collectionID"].(float64))}
	}
	return rec
}
