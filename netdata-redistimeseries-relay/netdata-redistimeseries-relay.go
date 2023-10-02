package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/goccy/go-json"

	//"github.com/mediocregopher/radix/v4"
	"github.com/redis/rueidis"
	//"github.com/redis/rueidis/internal/cmds"

	"context"
	"net"
)

// type datapoint struct {
// 	URL         string        `json:"url"`
// 	Title       string        `json:"title"`
// 	CustomShort string        `json:"short"`
// 	Expiry      time.Duration `json:"expiry"`
// 	Code        int           `json:"code"`
// 	Hash        string        `json:"hash"`
// 	Counter     int           `json:"counter"`
// }

// Program option vars:
var (
	listenAddress       string
	redisTimeSeriesHost string
	redisDelay          time.Duration
	logConn             string
	redisBulk           int

// Ctx                 = context.Background()
)

// Options:
func init() {
	flag.StringVar(&listenAddress, "listen-address", "127.0.0.1:8080", "The host:port for listening for JSON inputs")
	flag.StringVar(&redisTimeSeriesHost, "redistimeseries-host", "localhost:6379", "The host:port for Redis connection")
	flag.DurationVar(&redisDelay, "redis-delay", time.Millisecond*500, "Redis TS.ADD pipeline stagger at least that many milliseconds")
	flag.IntVar(&redisBulk, "redis-bulk", 5000, "Redis bulk TS.ADD buffer")
	flag.StringVar(&logConn, "connection-log", "detail", "Show per connection detailed log output - none, standard, detail")
	flag.Parse()
}

func server() {
	// listen on a port
	// vanillaClient, err := (radix.PoolConfig{}).New(Ctx, "tcp", redisTimeSeriesHost) // or any other client
	// if err != nil {
	// 	log.Fatalf("Error while creating new connection to %s. error = %v", redisTimeSeriesHost, err)
	// }
	rts, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisTimeSeriesHost}, MaxFlushDelay: redisDelay})
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
		go handleServerConnection(c, rts)
	}
}

func handleServerConnection(c net.Conn, client rueidis.Client) {
	defer c.Close()
	defer client.Close()
	reader := bufio.NewScanner(c)
	var rcv map[string]interface{}
	rem := c.RemoteAddr().String()
	cmds := make(rueidis.Commands, 0, redisBulk)
	delay := time.Now()
	reg, err := regexp.Compile("[^a-zA-Z0-9_./]+")
	if err != nil {
		log.Fatalf("Error while compiling regex. error = %v", err)
	}

	for reader.Scan() {
		line := reader.Bytes()
		err := json.Unmarshal(line, &rcv)
		if err != nil {
			log.Fatalf("Error while unmarshaling JSON. error = %v", err)
		}
		labels := make(map[string]string)
		//labelsKeys := []string{"prefix", "hostname", "chart_context", "chart_id", "chart_type", "chart_family", "chart_name", "id", "name", "units"}
		prefix := preProcessAndAddLabel(rcv, "prefix", reg, labels)
		hostname := preProcessAndAddLabel(rcv, "hostname", reg, labels)
		_ = preProcessAndAddLabel(rcv, "chart_context", reg, labels)
		_ = preProcessAndAddLabel(rcv, "chart_id", reg, labels)
		_ = preProcessAndAddLabel(rcv, "chart_type", reg, labels)
		chart_family := preProcessAndAddLabel(rcv, "chart_family", reg, labels)
		chart_name := preProcessAndAddLabel(rcv, "chart_name", reg, labels)
		_ = preProcessAndAddLabel(rcv, "id", reg, labels)
		metric_name := preProcessAndAddLabel(rcv, "name", reg, labels)
		_ = preProcessAndAddLabel(rcv, "units", reg, labels)

		value := rcv["value"].(float64)
		timestamp := strconv.FormatInt(int64(rcv["timestamp"].(float64)*1000.0), 10)

		//Metrics are sent to the database server as prefix:hostname:chart_family:chart_name:metric_name.
		keyName := prefix + ":" + hostname + ":" + chart_family + ":" + chart_name + ":" + metric_name
		//keyLabels := rueidis.Incomplete
		addCmd := client.B().TsAdd().Key(keyName).Timestamp(timestamp).Value(value).Labels()
		for key, label := range labels {
			addCmd.Labels(key, label)
		}
		//addCmd.Build()
		cmds = append(cmds, addCmd.Build())
		t1 := time.Now()
		l1 := len(cmds)
		if l1 > 0 && t1.After(delay.Add(redisDelay)) {
			for _, resp := range client.DoMulti(context.Background(), cmds...) {
				if err := resp.Error(); err != nil {
					log.Fatalf("Error while adding data points. error = %v", err)
				}
			}
			showLog(l1, hostname, rem, delay, t1, string(line))
			cmds = nil
			delay = time.Now()
		}
		//addCmd := radix.FlatCmd(nil, "TS.ADD", keyName, timestamp, value, labels)
		//rtsLabels := make(rueidis.Commands, 0, 10)
		//addCmd := client.Do(context.Background(), client.B().TsAdd().Key(keyName).Timestamp(string(timestamp)).Value(value).Labels().Labels("prefix", labels["prefix"]).)
		//p.Append(addCmd)
		//t1 := time.Now()
		//l1 := len(p.Properties().Keys)
		// if l1 > 0 && t1.After(delay.Add(redisDelay)) {
		// 	if err := client.Do(Ctx, p); err != nil {
		// 		log.Fatalf("Error while adding data points. error = %v", err)
		// 	}
		// 	showLog(l1, hostname, rem, delay, t1, string(line))
		// 	p.Reset()
		// 	delay = time.Now()
		// }
	}
}

func preProcessAndAddLabel(rcv map[string]interface{}, key string, reg *regexp.Regexp, labels map[string]string) (value string) {
	if rcv[key] != nil {
		value = reg.ReplaceAllString(rcv[key].(string), "")
		if len(value) > 0 {
			// if len(labelsOut) == 0 {
			// 	labelsOut = append(labelsOut, "LABELS")
			// }
			labels[key] = value
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
