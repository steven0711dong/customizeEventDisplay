/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

/*
Example Output:

☁️  cloudevents.Event
Validation: valid
Context Attributes,
  specversion: 1.0
  type: dev.knative.eventing.samples.heartbeat
  source: https://knative.dev/eventing-contrib/cmd/heartbeats/#event-test/mypod
  id: 2b72d7bf-c38f-4a98-a433-608fbcdd2596
  time: 2019-10-18T15:23:20.809775386Z
  contenttype: application/json
Extensions,
  beats: true
  heart: yes
  the: 42
Data,
  {
    "id": 2,
    "label": ""
  }
*/

var msg struct {
	data          map[string](map[int](map[string]time.Time))
	Mu            sync.Mutex
	totalMessages int
	timestamp     time.Time
}
var httpClient http.Client

func processEvents(event cloudevents.Event) {
	eventInfoArray := strings.Split(event.Context.GetID(), "/")
	topic := strings.Split(event.Context.GetSource(), "#")[1]
	partition, offset := strings.Split(eventInfoArray[0], ":")[1], strings.Split(eventInfoArray[1], ":")[1]
	p, err := strconv.Atoi(partition)
	if err != nil {
		fmt.Println("An error happened when converting partition to integer")
	}
	appendToData(topic, p, offset)
}

func appendToData(topic string, partition int, offset string) {
	msg.Mu.Lock()
	if partitions, found := msg.data[topic]; found {
		if offsets, ok := partitions[partition]; ok {
			if _, ok := offsets[offset]; ok {
				errStr := fmt.Sprintf("topic:%s-partition:%d-offset:%s", topic, partition, offset)
				offsets[errStr] = time.Now()
			} else {
				offsets[offset] = time.Now()
			}
		} else {
			newSetOfOffsets := make(map[string]time.Time)
			newSetOfOffsets[offset] = time.Now()
			partitions[partition] = newSetOfOffsets
		}
	} else {
		newSetOfOffsets := make(map[string]time.Time)
		newSetOfOffsets[offset] = time.Now()
		newPartitions := make(map[int](map[string]time.Time))
		newPartitions[partition] = newSetOfOffsets
		msg.data[topic] = newPartitions
	}
	msg.timestamp = time.Now()
	msg.Mu.Unlock()
}

func main() {
	if msg.data == nil {
		msg.data = make(map[string](map[int](map[string]time.Time)))
	}
	netHTTPTransport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     10,
	}
	httpClient.Transport = netHTTPTransport
	c, err := cloudevents.NewDefaultClient()
	if err != nil {
		log.Fatal("Failed to create client, ", err)
	}
	fmt.Println("Starting receiver...")
	go sendResult()
	log.Fatal(c.StartReceiver(context.Background(), processEvents))
}

func sendResult() {
	for {
		time.Sleep(time.Second * 30)
		msg.Mu.Lock()
		if time.Since(msg.timestamp) < time.Second*30 {
			msg.Mu.Unlock()
			continue
		}
		data, err := json.Marshal(msg.data)
		msg.Mu.Lock()
		if err != nil {
			fmt.Println("error streaming data", err.Error())
			return
		}
		req, err := http.NewRequest("GET", "https://scrape.7ud1ocu3uff.dev-pg1.codeengine.dev.appdomain.cloud", bytes.NewReader(data))
		if err != nil {
			fmt.Println("error creating request", err.Error())
			return
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			fmt.Println("error sending data", err.Error())
			return
		}
		responseString, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("err reading response body", err.Error())
			return
		}
		fmt.Println("Rcv'd response from scraper:", string(responseString))
	}
}

func displayResult() {
	time.Sleep(20 * time.Second)
	if time.Since(msg.timestamp) <= (time.Minute * 2) {
		return
	}
	msg.totalMessages = 0
	for topic, partitions := range msg.data {
		for partition, offsets := range partitions {
			msg.totalMessages += len(offsets)
			for offset := range offsets {
				fmt.Printf("Topic: %s => Partition: %d => Offset: %+v\n", topic, partition, offset)
			}
		}
	}
}

func displayMetrics() {
	fmt.Printf("A total of %d topics.\n", len(msg.data))
	fmt.Println("-------------------")
	for topic, partitions := range msg.data {
		fmt.Printf("Topic %s contains %d partitions.\n", topic, len(partitions))
	}
	fmt.Println("-------------------")
	fmt.Println("Total messages received: ", msg.totalMessages)
}

// func display(event cloudevents.Event) {
// 	time.Sleep(time.Second * 20)
// 	fmt.Printf("☁️  cloudevents.Event\n%s", event.String())
// }

// func main() {
// 	c, err := cloudevents.NewDefaultClient()
// 	if err != nil {
// 		log.Fatal("Failed to create client, ", err)
// 	}

// 	log.Fatal(c.StartReceiver(context.Background(), display))
// }

// const (
// 	//CURLCOMMAND constant for curl-command
// 	CURLCOMMAND = "curl-command"
// 	//SOURCE constant for display
// 	SOURCE = "display"
// )
//fmt.Printf("☁️  cloudevents.Event\n%s", event.String())
// if event.Context.GetSource() == CURLCOMMAND {
// 	if event.Context.GetID() == SOURCE {
// 		fmt.Println("displaying results...")
// 		displayResult()
// 		displayMetrics()
// 	} else {
// 		fmt.Println("resetting results...")
// 		msg.data = nil
// 		msg.totalMessages = 0
// 	}
// 	return
// }

//SAMPLE DATA FOR TESTING appendToData function
// a := "partition:0/offset:1548"
// b := "/apis/v1/namespaces/default/kafkasources/kafka-source-ibm-event-stream#kafka-java-console-sample-topic"
// c := "partition:0/offset:1549"
// d := "/apis/v1/namespaces/default/kafkasources/kafka-source-ibm-event-stream#kafka-java-console-sample-topic"
// e := "partition:0/offset:1549"
// f := "/apis/v1/namespaces/default/kafkasources/kafka-source-ibm-event-stream#kafka"
// j := "partition:1/offset:1548"
// k := "/apis/v1/namespaces/default/kafkasources/kafka-source-ibm-event-stream#kafka-java-console-sample-topic"
