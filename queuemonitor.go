// Package pulsesniffer provides a simple example program that listens to some
// real world pulse messages.
package main

import (
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
)

var (
	taskDataBucket = []byte("taskdata")
	tgId2HgRepoRev = []byte("tgid2hgreporev")
)

type QueueWatcher struct {
	InternalDB *bolt.DB
	MetricsDB  *bolt.DB
	Queue      *queue.Auth
}

func (qw QueueWatcher) run() {

	if os.Getenv("TASKCLUSTER_CLIENT_ID") == "" {
		log.Fatal("You must set (and export) environment variable TASKCLUSTER_CLIENT_ID.")
	}
	if os.Getenv("TASKCLUSTER_ACCESS_TOKEN") == "" {
		log.Fatal("You must set (and export) environment variable TASKCLUSTER_ACCESS_TOKEN.")
	}
	qw.Queue = queue.New(os.Getenv("TASKCLUSTER_CLIENT_ID"), os.Getenv("TASKCLUSTER_ACCESS_TOKEN"))
	// make sure bucket exists, before we update it later
	qw.InternalDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(taskDataBucket)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

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
	var err error
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
					revision = env["GECKO_HEAD_REV"].(string)
					err = qw.InternalDB.Update(func(tx *bolt.Tx) error {
						b := tx.Bucket(tgId2HgRepoRev)
						return b.Put([]byte(tgid+":"+tid), []byte(repository+":"+revision))
					})
					if err != nil {
						panic(err)
					}
				}
			}
		}
		if platform != "" {
			fmt.Printf("Task ID:        %v\n", tid)
			fmt.Printf("Task Graph ID:  %v\n", tgid)

			taskData := TaskData{
				Exchange:   delivery.Exchange,
				RoutingKey: delivery.RoutingKey,
				State:      state,
				Platform:   platform,
				Symbol:     fmt.Sprintf("%v", symbol),
				Scheduled:  scheduled,
				Started:    started,
				Resolved:   resolved,
			}

			// convert to json
			data, err := json.Marshal(taskData)
			if err != nil {
				panic(err)
			}

			err = qw.InternalDB.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(taskDataBucket)
				return b.Put([]byte(tid+":"+tgid), data)
			})
			if err != nil {
				panic(err)
			}
		}
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
}

func (qw QueueWatcher) processTaskGraph(status schedulerevents.TaskGraphStatusStructure, delivery amqp.Delivery) {
}
