package main

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
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
    tc-gecko-metrics-go run -o OUTPUT-METRICS -i INTERNAL-DATA -r HG-REPOS-JSON-FILE
    tc-gecko-metrics-go show -m MAPPING-FILE
    tc-gecko-metrics-go --help

Options:
    -h --help               Display this help text.
    -o OUTPUT-METRICS       BoltDB for metrics data
    -i INTERNAL-DATA        BoltDB to persist mappings of hg changeset -> pushlog
                            timestamp in. Useful if process crashes, so that
                            previously calculated values can be used on
                            subsequent run.
    -r HG-REPOS-JSON-FILE   A json file containing a list of hg repositories to
                            monitor. Each entry should have a "url" string and
                            a "name" string to describe the hg repository.
`
	metricsDB  *bolt.DB
	internalDB *bolt.DB
)

func main() {

	// parse the command line arguments
	arguments, err := docopt.Parse(usage, nil, true, version, false, true)
	if err != nil {
		panic(err)
	}

	// create new or open existing internal database
	internalDB, err = bolt.Open(arguments["-i"].(string), 0644, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		panic(err)
	}
	defer internalDB.Close()

	if arguments["show"].(bool) {
		internalDB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("hgcset2pushtime"))
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				fmt.Printf("key=%s, value=%s\n", k, v)
			}
			return nil
		})
		os.Exit(0)
	}

	// create new or open existing output database
	metricsDB, err = bolt.Open(arguments["-o"].(string), 0644, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		panic(err)
	}
	defer metricsDB.Close()

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
			InternalDB:   internalDB,
		}
		// sleep 7 seconds between each repo, so we don't hammer hg
		go rm.run()
		time.Sleep(time.Second * 7)
	}

	// monitor amqp queues for completed tasks and completed task graphs
	queueWatcher := QueueWatcher{
		InternalDB: internalDB,
		MetricsDB:  metricsDB,
	}
	go queueWatcher.run()

	// run forever...
	forever := make(chan bool)
	<-forever
}
