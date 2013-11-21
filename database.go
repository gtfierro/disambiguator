package main

import (
	"fmt"
	rethink "github.com/christopherhesse/rethinkgo"
	"sync/atomic"
)

var collectionCount = int32(0)

/**
  record {
      raw
      clean
      actual
      collectionID
      ID (rethinkdb reserved)
  }

  We want to iterate through the input file, and create a Record
  from each of the entries. As we loop through these Records, we want
  to insert them into RethinkDB with {raw, clean}. When inserting them
  into the DB, we want to loop through each group of records associated
  with each unique collectionID. If our record is sufficiently similar
  to any of them, we add it to Rethink with the same collectionID. If it's
  not, then we add it to Rethink under a new collectionID

*/

func runDisambiguations() {
	var response []interface{}
	for cc := 0; int32(cc) < collectionCount; cc += 1 {
		go func() {
			session, err := rethink.Connect("localhost:28015", "disambiguate")
			if err != nil {
				fmt.Println("err connection:", err)
				return
			}
			frequencies := make(map[string]int)
			rethink.Table("records").Filter(rethink.Map{"collectionID": cc}).Run(session).All(&response)
			for _, i := range response {
				r := convertInterfaceToRecord(i)
				frequencies[r.clean] += 1
			}
			actual := ""
			max := 0
			for str, count := range frequencies {
				if count > max {
					max = count
					actual = str
				}
			}
			var writeresp rethink.WriteResponse
			rethink.Table("records").Filter(rethink.Map{"collectionID": cc}).Update(rethink.Map{"actual": actual}).Run(session).All(&writeresp)
		}()
	}
}

func printOutput() {
	session, _ := rethink.Connect("localhost:28015", "disambiguate")
	response := rethink.Table("records").Run(session)
	i := 0
	for response.Next() {
		i += 1
		var r interface{}
		if err := response.Scan(&r); err != nil {
			fmt.Println("err:", err)
			break
		}
		rec := convertInterfaceToRecord(r)
		fmt.Print(rec.raw)
		fmt.Print("\t")
		fmt.Print(rec.actual)
		fmt.Print("\t")
		fmt.Println(rec.collectionID)

	}
}

func findMatch(rec *Record) {
	session, err := rethink.Connect("localhost:28015", "disambiguate")
	if err != nil {
		fmt.Println("err connection:", err)
		return
	}
	var response []interface{}
	for cc := 0; int32(cc) < collectionCount; cc += 1 {
		rethink.Table("records").Filter(rethink.Map{"collectionID": cc}).Run(session).All(&response)
		for _, i := range response {
			r := convertInterfaceToRecord(i)
			if rec.getRawSimilarity(r) >= .9 {
				addRecordToCollection(session, rec, int32(cc))
				return
			}
		}
	}
	// if no similarities found
	addRecord(session, rec)
}

func addRecord(session *rethink.Session, rec *Record) {
	var response rethink.WriteResponse
	atomic.AddInt32(&collectionCount, 1)
	insert := rethink.Map{"raw": rec.raw, "clean": rec.clean, "collectionID": collectionCount}
	err := rethink.Table("records").Insert(insert).Run(session).One(&response)
	if err != nil {
		fmt.Println(err)
	}
}

func addRecordToCollection(session *rethink.Session, rec *Record, collectionID int32) {
	var response rethink.WriteResponse
	insert := rethink.Map{"raw": rec.raw, "clean": rec.clean, "collectionID": collectionID}
	err := rethink.Table("records").Insert(insert).Run(session).One(&response)
	if err != nil {
		fmt.Println(err)
	}
}
