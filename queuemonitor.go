// Package pulsesniffer provides a simple example program that listens to some
// real world pulse messages.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/petemoore/pulse-go/pulse"
	"github.com/streadway/amqp"
	"github.com/taskcluster/taskcluster-client-go/queue"
	"github.com/taskcluster/taskcluster-client-go/queueevents"
	"github.com/taskcluster/taskcluster-client-go/schedulerevents"
	"log"
	"os"
	"strings"
)

var (
	TaskDataBucket = []byte("taskdata")
	TgId2HgRepoRev = []byte("tgid2hgreporev")
)

type QueueWatcher struct {
	InternalDB *bolt.DB
	MetricsDB  *bolt.DB
	Queue      *queue.Auth
}

func CreateBucket(db *bolt.DB, name []byte) {
	// make sure bucket exists, before we update it later
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(name)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
}

func (qw QueueWatcher) run() {

	if os.Getenv("TASKCLUSTER_CLIENT_ID") == "" {
		log.Fatal("You must set (and export) environment variable TASKCLUSTER_CLIENT_ID.")
	}
	if os.Getenv("TASKCLUSTER_ACCESS_TOKEN") == "" {
		log.Fatal("You must set (and export) environment variable TASKCLUSTER_ACCESS_TOKEN.")
	}
	qw.Queue = queue.New(os.Getenv("TASKCLUSTER_CLIENT_ID"), os.Getenv("TASKCLUSTER_ACCESS_TOKEN"))
	CreateBucket(qw.InternalDB, TaskDataBucket)
	CreateBucket(qw.InternalDB, TgId2HgRepoRev)

	// Passing all empty strings:
	// empty user => use PULSE_USERNAME env var
	// empty password => use PULSE_PASSWORD env var
	// empty url => connect to production
	conn := pulse.NewConnection("", "", "")
	conn.Consume(
		"metrics", // queue name
		func(message interface{}, delivery amqp.Delivery) { // callback function to pass messages to
			switch t := message.(type) {
			case *queueevents.TaskCompletedMessage:
				qw.processTask(t.Status, delivery)
			case *queueevents.TaskExceptionMessage:
				qw.processTask(t.Status, delivery)
			case *queueevents.TaskFailedMessage:
				qw.processTask(t.Status, delivery)
			case *schedulerevents.BlockedTaskGraphMessage:
				qw.processTaskGraph(t.Status, delivery)
			case *schedulerevents.TaskGraphFinishedMessage:
				qw.processTaskGraph(t.Status, delivery)
			default:
				fmt.Sprintf("Unrecognised message type %T!", t)
				os.Exit(3)
			}
			delivery.Ack(false) // acknowledge message *after* processing
		},
		1,     // prefetch 1 message at a time
		false, // don't auto-acknowledge messages
		queueevents.TaskCompleted{RoutingKeyKind: "primary"},
		queueevents.TaskException{RoutingKeyKind: "primary"},
		queueevents.TaskFailed{RoutingKeyKind: "primary"},
		schedulerevents.TaskGraphBlocked{RoutingKeyKind: "primary"},
		schedulerevents.TaskGraphFinished{RoutingKeyKind: "primary"},
	)
}

func (qw QueueWatcher) processTask(status queueevents.TaskStatusStructure, delivery amqp.Delivery) {
	if status.TaskId != "" {
		tid := status.TaskId
		tgid := status.TaskGroupId
		state := status.State.(string)
		lastRun := status.Runs[len(status.Runs)-1]
		scheduled := lastRun.Scheduled
		started := lastRun.Started
		resolved := lastRun.Resolved
		td, _ := qw.Queue.GetTask(tid)
		platform := ""
		var symbol interface{} = ""
		repository := ""
		revision := ""
		if td.Extra != nil {
			extra := td.Extra.(map[string]interface{})
			if extra["treeherder"] != nil {
				treeherder := extra["treeherder"].(map[string]interface{})
				if treeherder["machine"] != nil {
					machine := treeherder["machine"].(map[string]interface{})
					if machine["platform"] != nil {
						platform = machine["platform"].(string)
					}
				}
				if treeherder["symbol"] != nil {
					symbol = treeherder["symbol"]
				}
			}
		}
		if td.Payload != nil {
			payload := td.Payload.(map[string]interface{})
			if payload["env"] != nil {
				env := payload["env"].(map[string]interface{})
				if env["GECKO_HEAD_REPOSITORY"] != nil && env["GECKO_HEAD_REV"] != nil {
					repository = env["GECKO_HEAD_REPOSITORY"].(string)
					// fix up repo to match what we have in our config
					repository := strings.Replace(repository, "https", "http", 1)
					if repository[len(repository)-1] == '/' {
						repository = repository[:len(repository)-1]
					}
					revision = env["GECKO_HEAD_REV"].(string)
					pk := PushKey{ChangeSet: revision, RepoUrl: repository}
					data, err := json.Marshal(pk)
					if err != nil {
						panic(err)
					}
					err = qw.InternalDB.Update(func(tx *bolt.Tx) error {
						b := tx.Bucket(TgId2HgRepoRev)
						return b.Put([]byte(tgid), data)
					})
					if err != nil {
						panic(err)
					}
				}
			}
		}
		taskData := TaskData{
			TaskId:      tid,
			TaskGraphId: tgid,
			Exchange:    delivery.Exchange,
			RoutingKey:  delivery.RoutingKey,
			State:       state,
			Platform:    platform,
			Symbol:      fmt.Sprintf("%v", symbol),
			Scheduled:   scheduled,
			Started:     started,
			Resolved:    resolved,
		}

		fmt.Println("Adding new task data to database with TaskId: " + tid)

		// convert to json
		data, err := json.Marshal(taskData)
		if err != nil {
			panic(err)
		}

		err = qw.InternalDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(TaskDataBucket)
			return b.Put([]byte(tgid+":"+tid), data)
		})
		if err != nil {
			panic(err)
		}
		fmt.Println(" ... added.")
	} else {
		fmt.Println("Mysteriously received a queue event with a Status.TaskId of null")
		os.Exit(2)
	}
}

type TaskData struct {
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routingkey"`
	State      string `json:"state"`
	Platform   string `json:"platform"`
	Symbol     string `json:"symbol"`
	Scheduled  string `json:"scheduled"`
	Started    string `json:"started"`
	Resolved   string `json:"resolved"`

	TaskId      string `json:"taskId"`
	TaskGraphId string `json:"taskGraphId"`
	Repository  string `json:"repository"`
	ChangeSet   string `json:"changeset"`
	PushTime    string `json:"pushtime"`
}

func (qw QueueWatcher) processTaskGraph(status schedulerevents.TaskGraphStatusStructure, delivery amqp.Delivery) {
	tgid := []byte(status.TaskGraphId)
	fmt.Println("----------------------------------")
	fmt.Println("--- Received notification of a completed task graph...")
	fmt.Println("--- Task Group ID: " + string(tgid))
	qw.InternalDB.View(func(tx *bolt.Tx) error {
		repoRev := tx.Bucket(TgId2HgRepoRev).Get(tgid)
		if repoRev != nil {
			fmt.Println("--- Repo Rev: " + string(repoRev))
			// deserialise
			pk := PushKey{}
			err := json.Unmarshal(repoRev, &pk)
			if err != nil {
				panic(err)
			}
			// now join the hg repo data from the push log to the data collected from pulse...
			// now update all tasks with this taskGraphId to have the hg revision and repo
			qw.InternalDB.View(func(tx *bolt.Tx) error {
				fmt.Println("--- About to scan...")
				c := tx.Bucket(TaskDataBucket).Cursor()
				prefix := append(tgid, ':')
				fmt.Println("--- Prefix: " + string(prefix))
				for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
					fmt.Printf("--- key=%s, value=%s\n", k, v)
					// now deserialise
					td := TaskData{}
					err := json.Unmarshal(v, &td)
					if err != nil {
						panic(err)
					}
					td.Repository = pk.RepoUrl
					td.ChangeSet = pk.ChangeSet

					// now look up push time, maybe we processed it already
					qw.InternalDB.View(func(tx *bolt.Tx) error {
						b := tx.Bucket(hgCSet2PushTime)
						td.PushTime = string(b.Get(repoRev))
						fmt.Println("--- Push time: " + td.PushTime)
						return nil
					})

					// now serialise again
					out, err := json.Marshal(td)
					if err != nil {
						panic(err)
					}
					fmt.Printf("--- Out: %s\n", out)
					// now update
					err = qw.InternalDB.Update(func(tx *bolt.Tx) error {
						return tx.Bucket(TaskDataBucket).Put(k, out)
					})
					if err != nil {
						panic(err)
					}
				}
				return nil
			})
		} else {
			fmt.Println("--- Unknown repo revision for completed task graph id " + string(tgid) + ": not processing any further")
		}
		return nil
	})
}
