package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

var msg struct {
	data          map[string](map[int](map[string]time.Time))
	Mu            sync.Mutex
	totalMessages int
	dups          int
	timestamp     time.Time
}

var msg1 struct {
	data      []string
	Mu        sync.Mutex
	timestamp time.Time
}

var httpClient http.Client
var delay = getenv("delay", "3")
var scraperAdd = getenv("scraper", "")
var connCt uint64

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func processCloudEvents(event cloudevents.Event) {
	atomic.AddUint64(&connCt, 1)
	l := time.Now()
	d, _ := strconv.Atoi(delay)
	time.Sleep(time.Second * time.Duration(d))
	eventInfoArray := strings.Split(event.Context.GetID(), "/")
	topic := strings.Split(event.Context.GetSource(), "#")[1]
	partition, offset := strings.Split(eventInfoArray[0], ":")[1], strings.Split(eventInfoArray[1], ":")[1]
	p, err := strconv.Atoi(partition)
	if err != nil {
		fmt.Println("An error happened when converting partition to integer")
	}
	appendToData(topic, p, offset, l)
	atomic.AddUint64(&connCt, ^uint64(1-1))
}

func processEvents(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&connCt, 1)
	d, _ := strconv.Atoi(delay)
	time.Sleep(time.Second * time.Duration(d))
	partitionAndOffset := r.Header.Get("Ce-Id")
	eventInfoArray := strings.Split(partitionAndOffset, "/")
	topic := strings.Split(r.Header.Get("Ce-Source"), "#")[1]
	partition, offset := strings.Split(eventInfoArray[0], ":")[1], strings.Split(eventInfoArray[1], ":")[1]
	appendToData2(topic, partition, offset)
	atomic.AddUint64(&connCt, ^uint64(1-1))
}

func appendToData2(t string, p string, o string) {
	msg1.Mu.Lock()
	s := t + "/" + p + "/" + o
	msg1.data = append(msg1.data, s)
	msg1.Mu.Unlock()
}

func appendToData(topic string, partition int, offset string, t time.Time) {
	msg.Mu.Lock()
	if partitions, found := msg.data[topic]; found {
		if offsets, ok := partitions[partition]; ok {
			if _, ok := offsets[offset]; ok {
				msg.dups += 1
			} else {
				offsets[offset] = t
				msg.totalMessages += 1
			}
		} else {
			newSetOfOffsets := make(map[string]time.Time)
			newSetOfOffsets[offset] = t
			partitions[partition] = newSetOfOffsets
			msg.totalMessages += 1
		}
	} else {
		newSetOfOffsets := make(map[string]time.Time)
		newSetOfOffsets[offset] = t
		newPartitions := make(map[int](map[string]time.Time))
		newPartitions[partition] = newSetOfOffsets
		msg.data[topic] = newPartitions
		msg.totalMessages += 1
	}
	msg.Mu.Unlock()
}

func main() {
	if msg.data == nil {
		msg.data = make(map[string](map[int](map[string]time.Time)))
	}
	fmt.Println("Starting receiver...")
	go displayCon()
	go sendResult2()
	if os.Getenv("cloudevent") == "true" {
		c, err := cloudevents.NewDefaultClient()
		if err != nil {
			log.Fatal("Failed to create client, ", err)
		}
		log.Fatal(c.StartReceiver(context.Background(), processCloudEvents))
	} else {
		http.HandleFunc("/", processEvents)
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		log.Printf("listening on port %s", port)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
	}
}
func sendResult2() {
	for {
		time.Sleep(time.Second * 5)
		msg1.Mu.Lock()
		data, err := json.Marshal(msg1.data)
		msg1.data = []string{}
		msg1.Mu.Unlock()
		if err != nil {
			fmt.Println("error streaming data", err.Error())
			return
		}
		if scraperAdd == "" {
			fmt.Println("scraper address was not correct configured, check env setting for scraper")
			return
		}
		resp, err := http.Post(scraperAdd, "application/json", bytes.NewBuffer(data))
		if err != nil {
			fmt.Println("error sending data", err.Error())
			return
		}
		responseString, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("err reading response body", err.Error())
			return
		}
		fmt.Println("Rcv'd response from scraper: \n", string(responseString))
	}
}

func sendResult() {
	for {
		time.Sleep(time.Second * 5)
		msg.Mu.Lock()
		i := time.Now()
		data, err := json.Marshal(msg.data)
		msg.data = make(map[string]map[int]map[string]time.Time)
		fmt.Println("marshal:", int(time.Since(i).Seconds()))
		msg.Mu.Unlock()
		if err != nil {
			fmt.Println("error streaming data", err.Error())
			return
		}
		if scraperAdd == "" {
			fmt.Println("scraper address was not correct configured, check env setting for scraper")
			return
		}
		resp, err := http.Post(scraperAdd, "application/json", bytes.NewBuffer(data))
		if err != nil {
			fmt.Println("error sending data", err.Error())
			return
		}
		responseString, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("err reading response body", err.Error())
			return
		}
		fmt.Println("Rcv'd response from scraper: \n", string(responseString))
	}
}

func displayCon() {
	for {
		time.Sleep(1 * time.Second)
		fmt.Printf("Host %s, Currently concurrent connection number %d connCt\n", os.Getenv("HOSTNAME"), connCt)
		fmt.Print("------------------------------------------------------------\n")
	}
}
