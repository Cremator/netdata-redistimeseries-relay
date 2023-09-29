package main

import (
	"bufio"
	"flag"
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
)

// Program option vars:
var (
	listenAddress       string
	redisTimeSeriesHost string
	redisDelay          time.Duration
	logConn             string
)

// Options:
func init() {
	flag.StringVar(&listenAddress, "listen-address", "127.0.0.1:8080", "The host:port for listening for JSON inputs")
	flag.StringVar(&redisTimeSeriesHost, "redistimeseries-host", "localhost:6379", "The host:port for Redis connection")
	flag.DurationVar(&redisDelay, "redis-delay", time.Millisecond*500, "Redis TS.ADD pipeline stagger at least that many milliseconds")
	flag.StringVar(&logConn, "connection-log", "standard", "Show per connection detailed log output - none, standard, detail")
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
	log.Printf("Listening at %s for netdata JSON inputs, and pushing RedisTimeSeries datapoints to %s...\n", listenAddress, redisTimeSeriesHost)
	for {
		// accept a connection
		c, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		// handle the connection
		go handleServerConnection(c, vanillaClient, ctx)
	}
}

func handleServerConnection(c net.Conn, client radix.Client, ctx context.Context) {
	defer c.Close()
	defer client.Close()
	reader := bufio.NewScanner(c)
	var rcv map[string]interface{}
	rem := c.RemoteAddr().String()
	p := radix.NewPipeline()
	delay := time.Now()
	reg, err := regexp.Compile("[^a-zA-Z0-9_./]+")
	if err != nil {
		log.Fatal(err)
	}

	for reader.Scan() {
		line := reader.Bytes()
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
		keyName := prefix + ":" + hostname + ":" + chart_family + ":" + chart_name + ":" + metric_name
		addCmd := radix.FlatCmd(nil, "TS.ADD", keyName, timestamp, value, labels)
		p.Append(addCmd)
		t1 := time.Now()
		l1 := len(p.Properties().Keys)
		if l1 > 0 && t1.After(delay.Add(redisDelay)) {
			if err := client.Do(ctx, p); err != nil {
				log.Fatalf("Error while adding data points. error = %v", err)
			}
			showLog(l1, hostname, rem, delay, t1, string(line))
			p.Reset()
			delay = time.Now()
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

func showLog(l1 int, host string, rem string, delay time.Time, t1 time.Time, detailed string) {
	if logConn == "none" {
		return
	}
	log.Printf("Processed %d entries, %d ms since last data connection from %s - %s...\n", l1, t1.Sub(delay).Milliseconds(), host, rem)
	if logConn == "detailed" {
		log.Printf("%s\n", detailed)
	}
}

func main() {
	log.Println("Starting netdata-redistimeseries-relay...")
	go server()
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()
	<-done
	log.Println("Exiting...")
}
