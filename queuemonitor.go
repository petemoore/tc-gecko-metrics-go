// Package pulsesniffer provides a simple example program that listens to some
// real world pulse messages.
package main

import (
	// "encoding/json"
	"fmt"
	"github.com/petemoore/pulse-go/pulse"
	"github.com/streadway/amqp"
	"github.com/taskcluster/taskcluster-client-go/queue"
	"github.com/taskcluster/taskcluster-client-go/queueevents"
	"github.com/taskcluster/taskcluster-client-go/schedulerevents"
	"log"
	"os"
	"strings"
)

type QueueWatcher struct {
	Queue         *queue.Auth
	ReadResponse  chan interface{}
	WriteResponse chan bool
}

func (qw QueueWatcher) run() {

	qw.ReadResponse = make(chan interface{})
	qw.WriteResponse = make(chan bool)

	if os.Getenv("TASKCLUSTER_CLIENT_ID") == "" {
		log.Fatal("You must set (and export) environment variable TASKCLUSTER_CLIENT_ID.")
	}
	if os.Getenv("TASKCLUSTER_ACCESS_TOKEN") == "" {
		log.Fatal("You must set (and export) environment variable TASKCLUSTER_ACCESS_TOKEN.")
	}
	qw.Queue = queue.New(os.Getenv("TASKCLUSTER_CLIENT_ID"), os.Getenv("TASKCLUSTER_ACCESS_TOKEN"))

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
					fmt.Println("Writing Push Key for task graph id " + tgid + ":\n" + pk.String())
					PushKeyWrite <- &writeOp{
						key:  tgid,
						val:  pk,
						resp: qw.WriteResponse,
					}
					<-qw.WriteResponse
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

		fmt.Println("Adding new task data to database with TaskId " + tid + ":\n" + taskData.String())
		TaskDataWrite <- &writeOp{
			key:  tid,
			val:  taskData,
			resp: qw.WriteResponse,
		}
		<-qw.WriteResponse
		fmt.Println(" ... added.")
		TaskIdsRead <- &readOp{
			key:  tgid,
			resp: qw.ReadResponse,
		}
		s := []string{}
		NewTaskIds := make(map[string]bool)
		NewTaskIds[tid] = true
		s = append(s, tid)
		x := <-qw.ReadResponse
		if x != nil {
			taskIds := x.(map[string]bool)
			for k, v := range taskIds {
				NewTaskIds[k] = v
				if v {
					s = append(s, k)
				}
			}
		}
		fmt.Println("Writing new taskid list for task graph id " + tgid + ":")
		fmt.Println(strings.Join(s, ", "))
		TaskIdsWrite <- &writeOp{
			key:  tgid,
			val:  NewTaskIds,
			resp: qw.WriteResponse,
		}
		<-qw.WriteResponse
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

func (td *TaskData) String() string {
	return fmt.Sprintf(`
Exchange:      '%v'
Routing Key:   '%v'
State:         '%v'
Platform:      '%v'
Symbol:        '%v'
Scheduled:     '%v'
Started:       '%v'
Resolved:      '%v'
Task ID:       '%v'
Task Graph ID: '%v'
Repository:    '%v'
Change Set:    '%v'
Push Time:     '%v'
`,
		td.Exchange,
		td.RoutingKey,
		td.State,
		td.Platform,
		td.Symbol,
		td.Scheduled,
		td.Started,
		td.Resolved,
		td.TaskId,
		td.TaskGraphId,
		td.Repository,
		td.ChangeSet,
		td.PushTime,
	)
}

func (qw QueueWatcher) processTaskGraph(status schedulerevents.TaskGraphStatusStructure, delivery amqp.Delivery) {
	tgid := status.TaskGraphId
	fmt.Println("----------------------------------")
	fmt.Println("--- Received notification of a completed task graph...")
	fmt.Println("--- Task Group ID: " + tgid)
	// send message to channel in order to request read
	PushKeyRead <- &readOp{
		key:  tgid,
		resp: qw.ReadResponse,
	}
	// result of read is fed back to given channel
	x := <-qw.ReadResponse
	if x != nil {
		pk := x.(PushKey)
		fmt.Println("--- Repo Rev: " + pk.ChangeSet + ", " + pk.RepoUrl)
		// now join the hg repo data from the push log to the data collected from pulse...
		// now update all tasks with this taskGraphId to have the hg revision and repo
		TaskIdsRead <- &readOp{
			key:  tgid,
			resp: qw.ReadResponse,
		}
		x := <-qw.ReadResponse
		if x != nil {
			taskIds := x.(map[string]bool)
			for taskId := range taskIds {
				TaskDataRead <- &readOp{
					key:  taskId,
					resp: qw.ReadResponse,
				}
				td := (<-qw.ReadResponse).(TaskData)
				td.Repository = pk.RepoUrl
				td.ChangeSet = pk.ChangeSet

				// now look up push time, maybe we processed it already
				PushTimeRead <- &readOp{
					key:  pk,
					resp: qw.ReadResponse,
				}
				x2 := <-qw.ReadResponse
				if x2 != nil {
					td.PushTime = x2.(string)
				}

				fmt.Println("")
				fmt.Println("Writing task id " + taskId + ":\n" + td.String())
				fmt.Println("")
				TaskDataWrite <- &writeOp{
					key:  taskId,
					val:  td,
					resp: qw.WriteResponse,
				}
				<-qw.WriteResponse
			}
		}
	} else {
		fmt.Println("--- Unknown repo revision for completed task graph id " + string(tgid) + ": not processing any further")
	}
}
