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

	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

var msg1 struct {
	data      []string
	Mu        sync.Mutex
	timestamp time.Time
}

var httpClient http.Client
var delay = getenv("delay", "3")
var scraperAdd = getenv("scraper", "")
var connCt uint64
var logger *zap.SugaredLogger
var forward = os.Getenv("forward") == "true"
var displayConcurrentConnections = os.Getenv("displayConcurrentConnections") == "true"
var cloudevent = os.Getenv("cloudevent") == "true"
var print = os.Getenv("print") == "true"

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func processCloudEvents(event cloudevents.Event) {
	atomic.AddUint64(&connCt, 1)
	d, _ := strconv.Atoi(delay)
	time.Sleep(time.Second * time.Duration(d))
	eventInfoArray := strings.Split(event.Context.GetID(), "/")
	cesource := strings.Split(event.Context.GetSource(), "#")
	topic := cesource[1]
	kafkasourcearray := strings.Split(cesource[0], "/")
	source := kafkasourcearray[len(kafkasourcearray)-1]
	partition, offset := strings.Split(eventInfoArray[0], ":")[1], strings.Split(eventInfoArray[1], ":")[1]
	if forward {
		appendToData(source, topic, partition, offset)
	}
	printout := fmt.Sprintf("%s/%s/%s/%s", source, topic, partition, offset)
	if print {
		fmt.Println(printout)
	}
	atomic.AddUint64(&connCt, ^uint64(1-1))
}

func processEvents(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&connCt, 1)
	if delay != "0" {
		d, _ := strconv.Atoi(delay)
		time.Sleep(time.Second * time.Duration(d))
	}
	partitionAndOffset := r.Header.Get("Ce-Id")
	eventInfoArray := strings.Split(partitionAndOffset, "/")
	cesource := strings.Split(r.Header.Get("Ce-Source"), "#")
	topic := cesource[1]
	kafkasourcearray := strings.Split(cesource[0], "/")
	source := kafkasourcearray[len(kafkasourcearray)-1]
	partition, offset := strings.Split(eventInfoArray[0], ":")[1], strings.Split(eventInfoArray[1], ":")[1]
	if forward {
		appendToData(source, topic, partition, offset)
	}
	printout := fmt.Sprintf("%s/%s/%s/%s", source, topic, partition, offset)
	if print {
		fmt.Println(printout)
	}
	w.Write([]byte(printout))
	atomic.AddUint64(&connCt, ^uint64(1-1))
}

func appendToData(source string, t string, p string, o string) {
	msg1.Mu.Lock()
	s := source + "/" + t + "/" + p + "/" + o + "/" + "200"
	msg1.data = append(msg1.data, s)
	msg1.timestamp = time.Now()
	msg1.Mu.Unlock()
}

func main() {
	ctx, _ := context.WithCancel(context.Background())
	logger = logging.FromContext(ctx)
	logger.Infof("Env vars: displayConcurrentConnection: %t delay: %s cloudevent: %t forward: %t scraper: %s print %t \n", displayConcurrentConnections, delay, cloudevent, forward, scraperAdd, print)

	if displayConcurrentConnections {
		go displayCon()
	}

	if forward {
		go sendResult()
	}

	if cloudevent {
		logger.Info("Receiving events as cloudevents")
		c, err := cloudevents.NewDefaultClient()
		if err != nil {
			log.Fatal("Failed to create client, ", err)
		}
		log.Fatal(c.StartReceiver(context.Background(), processCloudEvents))
	} else {
		logger.Info("Receiving events as regular http requests")
		http.HandleFunc("/", processEvents)
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
	}
}

func sendResult() {
	for {
		time.Sleep(time.Second * 5)
		msg1.Mu.Lock()
		data, err := json.Marshal(msg1.data)
		msg1.data = []string{}
		msg1.Mu.Unlock()
		if err != nil {
			logger.Infof("Error streaming data: %s", err.Error())
			return
		}

		if scraperAdd == "" {
			logger.Info("scraper address was not correct configured, check env setting for scraper")
			return
		}

		retry := 0
		for retry < 3 {
			resp, err := http.Post(scraperAdd, "application/json", bytes.NewBuffer(data))
			if err != nil {
				logger.Infof("error sending data: %s\n", err.Error())
				retry++
				time.Sleep(time.Second * 2)
			} else {
				retry = 3
				responseString, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					logger.Infof("Err reading response body: %s", err.Error())
				}
				logger.Infof("%s rcv'd response from scraper status code: %d content: %s\n", os.Getenv("HOSTNAME"), resp.StatusCode, responseString)
				resp.Body.Close()
			}
		}
	}
}

func displayCon() {
	for {
		time.Sleep(1 * time.Second)
		logger.Infof("Host: %s: we have %d concurrent connections\n", os.Getenv("HOSTNAME"), connCt)
	}
}
