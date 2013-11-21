package main

import (
	r "github.com/christopherhesse/rethinkgo"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
)

var filechannel = make(chan []byte)
var m runtime.MemStats
var pairwisewg sync.WaitGroup
var recordwg sync.WaitGroup

func ingestRecords() {
	for raw := range filechannel {
		recordwg.Add(1)
		recordChannel <- makeRecord(raw)
	}
	recordwg.Wait()
	close(recordChannel)
}

func pairwiseComparison() {
	for i := 0; i < 100; i++ {
		pairwisewg.Add(1)
		go func() {
			for r := range recordChannel {
				findMatch(r)
				recordwg.Done()
			}
			pairwisewg.Done()
		}()
	}
	pairwisewg.Wait()
}

func main() {
	f, err := os.Create("disambiguator.cprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	runtime.GOMAXPROCS(runtime.NumCPU())

	session, err := r.Connect("localhost:28015", "disambiguate")
	var response r.WriteResponse
	r.Table("records").Delete().Run(session).One(&response)

	filename := os.Args[1]
	go readFile(filename, filechannel)
	go ingestRecords()
	pairwiseComparison()
	runDisambiguations()
	printOutput()
	filewg.Wait()

	f, err = os.Create("disambiguator.mprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.WriteHeapProfile(f)
	f.Close()
}
