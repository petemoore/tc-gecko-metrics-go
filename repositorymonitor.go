package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"
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
	HgRepository  HgRepository
	ReadResponse  chan interface{}
	WriteResponse chan bool
}

type HgRepository struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type HgRepositories []HgRepository

func (rm *RepositoryMonitor) run() {

	rm.ReadResponse = make(chan interface{})
	rm.WriteResponse = make(chan bool)

	// Simple implementation where we check every 15 seconds.
	// Please note something more complex could be done
	// here such as automatically adjusting the wait interval
	// based on frequency of discovered updates, e.g. when
	// there is little activity, we could increase interval
	// between polls dynamically.
	c := time.Tick(time.Second * 15)
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
		// check if we have it in our DB already
		PushTimeRead <- &readOp{
			key:  pkObj,
			resp: rm.ReadResponse,
		}
		x := <-rm.ReadResponse
		if x == nil {
			// we don't have data, let's add it...
			v := time.Unix(spl[i].Date, 0).In(time.UTC).Format("2006-01-02T15:04:05.999Z07:00")
			fmt.Println("Found new push for " + rm.HgRepository.Name + ": " + lastChangeSet + ": " + v)
			PushTimeWrite <- &writeOp{
				key:  pkObj,
				val:  v,
				resp: rm.WriteResponse,
			}
			<-rm.WriteResponse
		}
	}
}

type PushKey struct {
	RepoUrl   string `json:"repo"`
	ChangeSet string `json:"changeset"`
}

func (pk *PushKey) String() string {
	return fmt.Sprintf(`
Repo Url:  '%v'
Changeset: '%v'
`,
		pk.RepoUrl,
		pk.ChangeSet,
	)
}
