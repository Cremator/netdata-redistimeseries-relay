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
	"github.com/redis/rueidis"

	"context"
	"net"
)

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

func (d *datapoint) Insert(r rueidis.Client) error {
	value := d.Value
	timestamp := strconv.FormatInt(d.Timestamp*1000, 10)
	labels := d.Labels()
	keyName := labels["prefix"] + ":" + labels["hostname"] + ":" + labels["chart_family"] + ":" + labels["chart_name"] + ":" + labels["name"]
	rCmd := r.B().TsAdd().Key(keyName).Timestamp(timestamp).Value(value).Labels()
	for key, label := range labels {
		rCmd.Labels(key, label)
	}
	resp := r.Do(context.Background(), rCmd.Build())
	if err := resp.Error(); err != nil {
		return err
	}
	return nil
}

// Program option vars:
var (
	listenAddress       string
	redisTimeSeriesHost string
	redisDelay          time.Duration
	logConn             string
	redisBatch          int
)

func LookupEnvOrInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		v, err := strconv.Atoi(val)
		if err != nil {
			log.Fatalf("LookupEnvOrInt[%s]: %v", key, err)
		}
		return v
	}
	return defaultVal
}

func LookupEnvOrDuration(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok {
		v, err := time.ParseDuration(val)
		if err != nil {
			log.Fatalf("LookupEnvOrDuration[%s]: %v", key, err)
		}
		return v
	}
	return defaultVal
}

func LookupEnvOrString(key string, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

// Options:
func init() {
	flag.StringVar(&listenAddress, "listen-address", LookupEnvOrString("LISTEN_ADDRESS", "127.0.0.1:8080"), "The host:port for listening for JSON inputs")
	flag.StringVar(&redisTimeSeriesHost, "redistimeseries-host", LookupEnvOrString("REDIS_ADDRESS", "localhost:6379"), "The host:port for Redis connection")
	flag.DurationVar(&redisDelay, "redis-delay", LookupEnvOrDuration("REDIS_DELAY", time.Millisecond*500), "Delay and accumulate Redis TS.ADDs duration")
	flag.IntVar(&redisBatch, "redis-batch", LookupEnvOrInt("REDIS_BATCH", 5000), "Redis TS.ADDs max batch size")
	flag.StringVar(&logConn, "connection-log", LookupEnvOrString("CONN_LOG", "standard"), "Show per connection detailed log - none, standard, detail")
	flag.Parse()
}

func rtsClient() rueidis.Client {
	//r, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisTimeSeriesHost}, MaxFlushDelay: redisDelay})
	r, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:   []string{redisTimeSeriesHost},
		MaxFlushDelay: 50 * time.Microsecond,
	})
	if err != nil {
		log.Fatalf("Error while creating new connection to %s. error = %v", redisTimeSeriesHost, err)
	}
	return r
}

func server() {
	//r, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisTimeSeriesHost}, MaxFlushDelay: redisDelay})
	// r, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisTimeSeriesHost}})
	// if err != nil {
	// 	log.Fatalf("Error while creating new connection to %s. error = %v", redisTimeSeriesHost, err)
	// }
	r := rtsClient()
	s, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("Error while trying to listen to %s. error = %v", listenAddress, err)
		return
	}
	log.Printf("Listening at %s for netdata JSON inputs, and pushing RedisTimeSeries datapoints to %s...\n", listenAddress, redisTimeSeriesHost)
	log.Printf("Configured redis delay is %s, netdata JSON connection batch size is %d, and logs %s...\n", redisDelay, redisBatch, logConn)
	for {
		// accept a connection
		c, err := s.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		// handle the connection
		go handleServerConnection(c, r)
	}
}

func handleServerConnection(c net.Conn, r rueidis.Client) {
	defer c.Close()
	defer r.Close()
	//tnow := time.Now()
	reader := bufio.NewScanner(c)
	reader.Split(bufio.ScanLines)
	rcv := datapoint{}
	//rem := c.RemoteAddr().String()
	//cmds := make(rueidis.Commands, 0, redisBatch)

	for reader.Scan() {
		line := reader.Bytes()
		err := json.Unmarshal(line, &rcv)
		if err != nil {
			log.Fatalf("Error while unmarshaling JSON. error = %v", err)
		}
		rcv.Insert(r)
		// value := rcv.Value
		// timestamp := strconv.FormatInt(rcv.Timestamp*1000, 10)
		// labels := rcv.Labels()
		// //Metrics are sent to the database server as prefix:hostname:chart_family:chart_name:metric_name.
		// keyName := labels["prefix"] + ":" + labels["hostname"] + ":" + labels["chart_family"] + ":" + labels["chart_name"] + ":" + labels["name"]
		// addCmd := r.B().TsAdd().Key(keyName).Timestamp(timestamp).Value(value).Labels()
		// for key, label := range labels {
		// 	addCmd.Labels(key, label)
		// }
		// cmds = append(cmds, addCmd.Build())
		// t1 := time.Now()
		// l1 := len(cmds)
		// if (l1 > 0 && t1.After(tnow.Add(redisDelay))) || l1 >= redisBatch-1 {
		// 	for _, resp := range r.DoMulti(context.Background(), cmds...) {
		// 		if err := resp.Error(); err != nil {
		// 			log.Fatalf("Error while adding data points. error = %v", err)
		// 		}
		// 	}
		// 	showLog(l1, rcv.Hostname, rem, tnow, t1, string(line))
		// 	cmds = nil
		// 	tnow = time.Now()
		// }
	}
}

// func showLog(l1 int, host string, rem string, delay time.Time, t1 time.Time, line string) {
// 	if logConn == "none" {
// 		return
// 	}
// 	log.Printf("Processed %d entries, %s since last data processing from %s(%s)\n", l1, t1.Sub(delay), host, rem)
// 	if logConn == "detail" {
// 		log.Printf("Details: %s\n", line)
// 	}
// }

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
