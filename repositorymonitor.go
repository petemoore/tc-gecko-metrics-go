package main

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"
)

var (
	hgCSet2PushTime = []byte("hgcset2pushtime")
)

type PushLogEntry struct {
	ChangeSets []string `json:"changesets"`
	Date       int64    `json:"date"`
	User       string   `json:"user"`
	ID         uint32
}

type SortedPushLog []PushLogEntry

// needed so that SortedPushLog can implement sort.Interface
func (a SortedPushLog) Len() int           { return len(a) }
func (a SortedPushLog) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedPushLog) Less(i, j int) bool { return a[i].ID < a[j].ID }

type RepositoryMonitor struct {
	HgRepository HgRepository
	InternalDB   *bolt.DB
}

type HgRepository struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type HgRepositories []HgRepository

func (rm *RepositoryMonitor) run() {
	CreateBucket(rm.InternalDB, hgCSet2PushTime)

	// Simple implementation where we check every minute
	// please note something more complex could be done
	// here such as automatically adjusting the wait interval
	// based on frequency of discovered updates, e.g. when
	// there is little activity, we could increase interval
	// between polls dynamically.
	c := time.Tick(time.Minute * 1)
	for range c {
		rm.checkPushLog()
	}
	fmt.Println("For some reason, the push log checking loop has exited for repo " + rm.HgRepository.Name + " (" + rm.HgRepository.URL + ")")
	os.Exit(1)
}

func (rm *RepositoryMonitor) checkPushLog() {
	fmt.Println("Checking push log for repo " + rm.HgRepository.Name + "...")
	resp, err := http.Get(rm.HgRepository.URL + "/json-pushes")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	bodyReader := resp.Body
	decoder := json.NewDecoder(bodyReader)
	data := make(map[string]PushLogEntry)
	err = decoder.Decode(&data)
	if err != nil {
		panic(err)
	}
	spl := SortedPushLog(make([]PushLogEntry, 0, len(data)))
	for i := range data {
		entry := data[i]
		ID, err := strconv.ParseUint(i, 10, 32)
		entry.ID = uint32(ID)
		if err != nil {
			panic(err)
		}
		spl = append(spl, entry)
	}
	sort.Sort(spl)
	// loop through pushes
	for i := range spl {
		// get last changeset per push
		lastChangeSet := spl[i].ChangeSets[len(spl[i].ChangeSets)-1]
		// create a key for storing this data
		pkObj := PushKey{
			RepoUrl:   rm.HgRepository.URL,
			ChangeSet: lastChangeSet,
		}
		// convert key to json
		key, err := json.Marshal(pkObj)
		if err != nil {
			panic(err)
		}
		// check if we have it in our DB already
		err = rm.InternalDB.View(func(tx *bolt.Tx) error {
			v := tx.Bucket(hgCSet2PushTime).Get(key)
			if v == nil {
				// we don't have data, let's add it...
				fmt.Println("Found new push for " + rm.HgRepository.Name + ": " + lastChangeSet)
				return rm.InternalDB.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket(hgCSet2PushTime)
					return b.Put(key, []byte(time.Unix(spl[i].Date, 0).In(time.UTC).Format("2006-01-02T15:04:05.999Z07:00")))
				})
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}

type PushKey struct {
	RepoUrl   string `json:"repo"`
	ChangeSet string `json:"changeset"`
}
