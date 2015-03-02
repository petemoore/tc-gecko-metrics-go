// Package pulsesniffer provides a simple example program that listens to some
// real world pulse messages.
package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/petemoore/pulse-go/pulse"
	"github.com/streadway/amqp"
	"github.com/taskcluster/taskcluster-client-go/queue"
	"github.com/taskcluster/taskcluster-client-go/queueevents"
	"os"
)

type QueueWatcher struct {
	InternalDB *bolt.DB
	MetricsDB  *bolt.DB
}

func (qw QueueWatcher) run() {

	if os.Getenv("TASKCLUSTER_CLIENT_ID") == "" {
		log.Fatal("You must set (and export) environment variable TASKCLUSTER_CLIENT_ID.")
	}
	if os.Getenv("TASKCLUSTER_ACCESS_TOKEN") == "" {
		log.Fatal("You must set (and export) environment variable TASKCLUSTER_ACCESS_TOKEN.")
	}
	Queue := queue.New(os.Getenv("TASKCLUSTER_CLIENT_ID"), os.Getenv("TASKCLUSTER_ACCESS_TOKEN"))

	// Passing all empty strings:
	// empty user => use PULSE_USERNAME env var
	// empty password => use PULSE_PASSWORD env var
	// empty url => connect to production
	conn := pulse.NewConnection("", "", "")
	conn.Consume(
		"metrics", // queue name
		func(message interface{}, delivery amqp.Delivery) { // callback function to pass messages to
			var status queueevents.TaskStatusStructure
			switch t := message.(type) {
			case *queueevents.TaskCompletedMessage:
				status = t.Status
			case *queueevents.TaskExceptionMessage:
				status = t.Status
			case *queueevents.TaskFailedMessage:
				status = t.Status
			default:
				fmt.Sprintf("Unrecognised message type %T!", t)
			}

			if status.TaskId != "" {
				tid := status.TaskId
				tgid := status.TaskGroupId
				state := status.State.(string)
				lastRun := status.Runs[len(status.Runs)-1]
				scheduled := lastRun.Scheduled
				started := lastRun.Started
				resolved := lastRun.Resolved
				td, _ := Queue.GetTask(tid)
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
						if env["GECKO_HEAD_REPOSITORY"] != nil {
							repository = env["GECKO_HEAD_REPOSITORY"].(string)
						}
						if env["GECKO_HEAD_REV"] != nil {
							revision = env["GECKO_HEAD_REV"].(string)
						}
					}
				}
				if platform != "" && repository != "" && revision != "" {
					// fmt.Println(string(delivery.Body))
					fmt.Printf("Task ID:        %v\n", tid)
					fmt.Printf("Task Graph ID:  %v\n", tgid)
					fmt.Printf("Exchange:       %v\n", delivery.Exchange)
					fmt.Printf("Routing Key:    %v\n", delivery.RoutingKey)
					fmt.Printf("State:          %v\n", state)
					fmt.Printf("Platform:       %v\n", platform)
					fmt.Printf("Symbol:         %v\n", symbol)
					fmt.Printf("Repository:     %v\n", repository)
					fmt.Printf("Revision:       %v\n", revision)
					fmt.Printf("Scheduled:      %v\n", scheduled)
					fmt.Printf("Started:        %v\n", started)
					fmt.Printf("Resolved:       %v\n", resolved)
					fmt.Printf("State:          %v\n", state)
					fmt.Println("")
				}
			}

			delivery.Ack(false) // acknowledge message *after* processing
		},
		1,     // prefetch 1 message at a time
		false, // don't auto-acknowledge messages
		// queueevents.ArtifactCreated{},
		queueevents.TaskCompleted{RoutingKeyKind: "primary"},
		// queueevents.TaskDefined{},
		queueevents.TaskException{RoutingKeyKind: "primary"},
		queueevents.TaskFailed{RoutingKeyKind: "primary"},
		// queueevents.TaskPending{},
		// queueevents.TaskRunning{},
		// schedulerevents.TaskGraphBlocked{},
		// schedulerevents.TaskGraphExtended{},
		// schedulerevents.TaskGraphFinished{},
		// schedulerevents.TaskGraphRunning{},
	)
}
