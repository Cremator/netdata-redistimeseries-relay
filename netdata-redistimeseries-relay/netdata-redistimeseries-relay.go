package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/goccy/go-json"
	"github.com/mediocregopher/radix/v4"

	"context"
	"net"
	"net/textproto"
)

// Program option vars:
var (
	listenAddress       string
	redisTimeSeriesHost string
	//poolPipelineConcurrency int
	//poolPipelineWindow      time.Duration
)

// Options:
func init() {
	flag.StringVar(&listenAddress, "listen-address", "127.0.0.1:8080", "The host:port for listening for JSON inputs")
	flag.StringVar(&redisTimeSeriesHost, "redistimeseries-host", "localhost:6379", "The host:port for Redis connection")
	//flag.DurationVar(&poolPipelineWindow, "pipeline-window-ms", time.Millisecond*0, "If window is zero then implicit pipelining will be disabled")
	//flag.IntVar(&poolPipelineConcurrency, "pipeline-max-size", 0, "If limit is zero then no limit will be used and pipelines will only be limited by the specified time window")
	flag.Parse()
}

func server() {
	// listen on a port
	var ctx = context.Background()
	vanillaClient, err := (radix.PoolConfig{}).New(ctx, "tcp", redisTimeSeriesHost) // or any other client
	if err != nil {
		log.Fatalf("Error while creating new connection to %s. error = %v", redisTimeSeriesHost, err)
	}

	ln, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("Error while trying to listen to %s. error = %v", listenAddress, err)
		return
	}
	fmt.Printf("Listening at %s for JSON inputs, and pushing RedisTimeSeries datapoints to %s...\n", listenAddress, redisTimeSeriesHost)
	for {
		// accept a connection
		c, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		// handle the connection
		go handleServerConnection(c, vanillaClient, ctx)
	}
}

func handleServerConnection(c net.Conn, client radix.Client, ctx context.Context) {
	defer c.Close()

	reader := bufio.NewReader(c)
	tp := textproto.NewReader(reader)
	var rcv map[string]interface{}
	rem := c.RemoteAddr().String()
	p := radix.NewPipeline()
	delay := time.Now().UnixMilli()
	reg, err := regexp.Compile("[^a-zA-Z0-9_./]+")
	if err != nil {
		log.Fatal(err)
	}

	defer c.Close()
	for {
		line, err := tp.ReadLineBytes()
		if err == nil {
			json.Unmarshal(line, &rcv)
			var labels []string = nil
			prefix, labels := preProcessAndAddLabel(rcv, "prefix", reg, labels)
			hostname, labels := preProcessAndAddLabel(rcv, "hostname", reg, labels)
			_, labels = preProcessAndAddLabel(rcv, "chart_context", reg, labels)
			_, labels = preProcessAndAddLabel(rcv, "chart_id", reg, labels)
			_, labels = preProcessAndAddLabel(rcv, "chart_type", reg, labels)
			chart_family, labels := preProcessAndAddLabel(rcv, "chart_family", reg, labels)
			chart_name, labels := preProcessAndAddLabel(rcv, "chart_name", reg, labels)
			_, labels = preProcessAndAddLabel(rcv, "id", reg, labels)
			metric_name, labels := preProcessAndAddLabel(rcv, "name", reg, labels)
			_, labels = preProcessAndAddLabel(rcv, "units", reg, labels)

			value := rcv["value"].(float64)
			timestamp := int64(rcv["timestamp"].(float64) * 1000.0)

			//Metrics are sent to the database server as prefix:hostname:chart_family:chart_name:metric_name.
			keyName := fmt.Sprintf("%s:%s:%s:%s:%s", prefix, hostname, chart_family, chart_name, metric_name)
			addCmd := radix.FlatCmd(nil, "TS.ADD", keyName, timestamp, value, labels)
			p.Append(addCmd)
			if len(p.Properties().Keys) > 0 && time.Now().UnixMilli() >= delay+500 {
				if err := client.Do(ctx, p); err != nil {
					log.Fatalf("Error while adding data points. error = %v", err)
				}
				fmt.Printf("INFO - %s - Processing %d entries for %s...\n", time.Now(), len(p.Properties().Keys), rem)
				p.Reset()
				//t1 = time.Now()
				if err != nil {
					log.Fatalf("Error while adding data points. error = %v", err)
				}
			}
		}
	}

}

func preProcessAndAddLabel(rcv map[string]interface{}, key string, reg *regexp.Regexp, labels []string) (value string, labelsOut []string) {
	labelsOut = labels
	if rcv[key] != nil {
		value = reg.ReplaceAllString(rcv[key].(string), "")
		if len(value) > 0 {
			if len(labelsOut) == 0 {
				labelsOut = append(labelsOut, "LABELS")
			}
			labelsOut = append(labelsOut, key, value)
		}
	}
	return
}

func main() {
	fmt.Println("Starting netdata-redistimeseries-relay...")
	go server()
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()
	<-done
	fmt.Println("Exiting...")
}
