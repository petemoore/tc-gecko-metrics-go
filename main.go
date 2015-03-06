package main

import (
	"encoding/json"
	"fmt"
	docopt "github.com/docopt/docopt-go"
	"os"
	"time"
)

var (
	version = "tc-gecko-metrics-go 1.0"
	usage   = `
tc-gecko-metrics-go
tc-gecko-metrics-go ..... blah blah blah ....

Usage:
    tc-gecko-metrics-go run -r HG-REPOS-JSON-FILE
    tc-gecko-metrics-go show
    tc-gecko-metrics-go --help

Options:
    -h --help               Display this help text.
    -r HG-REPOS-JSON-FILE   A json file containing a list of hg repositories to
                            monitor. Each entry should have a "url" string and
                            a "name" string to describe the hg repository.
`
	TaskDataRead  chan (*readOp)
	TaskDataWrite chan (*writeOp)

	PushKeyRead  chan (*readOp)
	PushKeyWrite chan (*writeOp)

	TaskIdsRead  chan (*readOp)
	TaskIdsWrite chan (*writeOp)

	PushTimeRead  chan (*readOp)
	PushTimeWrite chan (*writeOp)
)

// Represents a read operation from a map shared by multiple go routines
// so that read/write operations are centrally managed by a single go
// routine.
type readOp struct {
	key  interface{}
	resp chan interface{}
}

// Represents a write operation from a map shared by multiple go routines
// so that read/write operations are centrally managed by a single go
// routine.
type writeOp struct {
	key  interface{}
	val  interface{}
	resp chan bool
}

// This function starts a go routine for managing read/writes to a map. It
// returns two channels - a channel to request reads from and a channel to
// request writes to the map.
func NewMapProxy() (reads chan (*readOp), writes chan (*writeOp)) {
	state := make(map[interface{}]interface{})
	reads = make(chan *readOp)
	writes = make(chan *writeOp)
	fmt.Println("Creating go function for managing a map...")
	go func() {
		for {
			select {
			case read := <-reads:
				// fmt.Printf("Reading: key='%v', value='%v'\n", read.key, state[read.key])
				read.resp <- state[read.key]
			case write := <-writes:
				// fmt.Printf("Writing: key='%v', value='%v'\n", write.key, write.val)
				state[write.key] = write.val
				write.resp <- true
			}
		}
	}()
	return
}

func main() {
	// parse the command line arguments
	arguments, err := docopt.Parse(usage, nil, true, version, false, true)
	if err != nil {
		panic(err)
	}

	if arguments["show"].(bool) {
		// show(hgCSet2PushTime)
		// show(TgId2HgRepoRev)
		// show(TaskDataBucket)
		os.Exit(0)
	}

	// start go routines to manage global maps
	TaskDataRead, TaskDataWrite = NewMapProxy()
	PushKeyRead, PushKeyWrite = NewMapProxy()
	TaskIdsRead, TaskIdsWrite = NewMapProxy()
	PushTimeRead, PushTimeWrite = NewMapProxy()

	// read in the input file to see which repositories should be monitored
	hgRepositories := HgRepositories{}
	fileReader, err := os.Open(arguments["-r"].(string))
	if err != nil {
		panic(err)
	}
	jsonDecoder := json.NewDecoder(fileReader)
	err = jsonDecoder.Decode(&hgRepositories)
	if err != nil {
		panic(err)
	}

	// start a go routine for each repository monitor
	for i := range hgRepositories {
		rm := RepositoryMonitor{
			HgRepository: hgRepositories[i], // data about which push log to monitor
		}
		// sleep 1 second between each repo, so we don't hammer hg
		repo := hgRepositories[i]
		fmt.Println("Creating a repository monitor for " + repo.Name + "...")
		go rm.run()
		time.Sleep(time.Second * 1)
	}

	// monitor amqp queues for completed tasks and completed task graphs
	queueWatcher := QueueWatcher{}
	fmt.Println("Creating a queue watcher...")
	go queueWatcher.run()

	// run forever...
	forever := make(chan bool)
	<-forever
}
