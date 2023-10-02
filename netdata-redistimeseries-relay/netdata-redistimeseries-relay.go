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

// {
// 	"prefix":"netdata",
// 	"hostname":"xcp-bld-006",
// 	"chart_id":"disk_qops.dm-3",
// 	"chart_name":"disk_qops.vg_xenstorage__547a9c93__686a__ea48__3be8__e17dfbd6aa9f_vhd__56bc486a__0177__49a0__82f2__7749361408ef",
// 	"chart_family":"vg_xenstorage--547a9c93--686a--ea48--3be8--e17dfbd6aa9f-vhd--56bc486a--0177--49a0--82f2--7749361408ef",
// 	"chart_context": "disk.qops",
// 	"chart_type":"disk_qops",
// 	"units": "operations",
// 	"id":"operations",
// 	"name":"operations",
// 	"value":0.0000000,
// 	"timestamp": 1696249227
// }

type datapoint struct {
	Prefix        string  `json:"prefix"`
	Hostname      string  `json:"hostname"`
	Chart_ID      string  `json:"chart_id"`
	Chart_Name    string  `json:"chart_name"`
	Chart_Family  string  `json:"chart_family"`
	Chart_Context string  `json:"chart_context"`
	Chart_Type    string  `json:"chart_type"`
	Units         string  `json:"units"`
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	Value         float64 `json:"value"`
	Timestamp     int64   `json:"timestamp"`
}

func (d *datapoint) Labels() map[string]string {
	reg, err := regexp.Compile("[^a-zA-Z0-9_./]+")
	if err != nil {
		log.Fatalf("Error while compiling regex. error = %v", err)
	}
	labels := map[string]string{"prefix": reg.ReplaceAllString(d.Prefix, ""), "hostname": reg.ReplaceAllString(d.Hostname, ""), "chart_id": reg.ReplaceAllString(d.Chart_ID, ""), "chart_name": reg.ReplaceAllString(d.Chart_Name, ""), "chart_family": reg.ReplaceAllString(d.Chart_Family, ""), "chart_context": reg.ReplaceAllString(d.Chart_Context, ""), "chart_type": reg.ReplaceAllString(d.Chart_Type, ""), "units": reg.ReplaceAllString(d.Units, ""), "id": reg.ReplaceAllString(d.ID, ""), "name": reg.ReplaceAllString(d.Name, "")}
	return labels
}

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
	//var rcv map[string]interface{}
	var rcv datapoint
	rem := c.RemoteAddr().String()
	cmds := make(rueidis.Commands, 0, redisBulk)
	delay := time.Now()
	// reg, err := regexp.Compile("[^a-zA-Z0-9_./]+")
	// if err != nil {
	// 	log.Fatalf("Error while compiling regex. error = %v", err)
	// }

	for reader.Scan() {
		line := reader.Bytes()
		err := json.Unmarshal(line, &rcv)
		if err != nil {
			log.Fatalf("Error while unmarshaling JSON. error = %v", err)
		}
		//labels := make(map[string]string)
		//labelsKeys := []string{"prefix", "hostname", "chart_context", "chart_id", "chart_type", "chart_family", "chart_name", "id", "name", "units"}
		// prefix := preProcessAndAddLabel(rcv, "prefix", reg, labels)
		// hostname := preProcessAndAddLabel(rcv, "hostname", reg, labels)
		// _ = preProcessAndAddLabel(rcv, "chart_context", reg, labels)
		// _ = preProcessAndAddLabel(rcv, "chart_id", reg, labels)
		// _ = preProcessAndAddLabel(rcv, "chart_type", reg, labels)
		// chart_family := preProcessAndAddLabel(rcv, "chart_family", reg, labels)
		// chart_name := preProcessAndAddLabel(rcv, "chart_name", reg, labels)
		// _ = preProcessAndAddLabel(rcv, "id", reg, labels)
		// metric_name := preProcessAndAddLabel(rcv, "name", reg, labels)
		// _ = preProcessAndAddLabel(rcv, "units", reg, labels)
		// err = preProcessStringsRegEx(rcv, reg)
		// if err != nil {
		// 	log.Fatalf("Error while pre processing JSON. error = %v", err)
		// }
		value := rcv.Value
		timestamp := strconv.FormatInt(int64(rcv.Timestamp*1000.0), 10)

		//Metrics are sent to the database server as prefix:hostname:chart_family:chart_name:metric_name.
		keyName := rcv.Prefix + ":" + rcv.Hostname + ":" + rcv.Chart_Family + ":" + rcv.Chart_Name + ":" + rcv.Name
		//keyLabels := rueidis.Incomplete
		addCmd := client.B().TsAdd().Key(keyName).Timestamp(timestamp).Value(value).Labels()
		for key, label := range rcv.Labels() {
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
			showLog(l1, rcv.Hostname, rem, delay, t1, string(line))
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

// func preProcessAndAddLabel(rcv map[string]interface{}, key string, reg *regexp.Regexp, labels map[string]string) (value string) {
// 	if rcv[key] != nil {
// 		value = reg.ReplaceAllString(rcv[key].(string), "")
// 		if len(value) > 0 {
// 			// if len(labelsOut) == 0 {
// 			// 	labelsOut = append(labelsOut, "LABELS")
// 			// }
// 			labels[key] = value
// 		}
// 	}
// 	return
// }

// func preProcessStringsRegEx(rcv datapoint, reg *regexp.Regexp) (err error) {
// 	for label, value := range rcv.Labels {
// 		if value != "" {
// 			rcv.Labels[label] = reg.ReplaceAllString(value, "")
// 		}
// 	}

// 	return err
// }

func showLog(l1 int, host string, rem string, delay time.Time, t1 time.Time, line string) {
	if logConn == "none" {
		return
	}
	log.Printf("Processed %d entries, %d ms since last data connection from %s - %s...\n", l1, t1.Sub(delay).Milliseconds(), host, rem)
	if logConn == "detail" {
		log.Printf("Details: %s\n", line)
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
