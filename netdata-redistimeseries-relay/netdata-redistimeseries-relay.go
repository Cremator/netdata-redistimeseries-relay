package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync/atomic"
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
	Timestamp_Str string
	Labels        map[string]string
	Keyname       string
}

// type redatapoint struct {
// 	Keyname   string
// 	Timestamp string
// 	Labels    map[string]string
// }

type datapointJSON datapoint

func (j *datapointJSON) UnmarshalJSON(b []byte) error {
	if err := json.Unmarshal(b, (*datapoint)(j)); err != nil {
		return err
	}
	reg, err := regexp.Compile("[^a-zA-Z0-9_./]+")
	if err != nil {
		return err
	}
	j.Labels = map[string]string{"prefix": reg.ReplaceAllString(j.Prefix, ""), "hostname": reg.ReplaceAllString(j.Hostname, ""), "chart_id": reg.ReplaceAllString(j.Chart_ID, ""), "chart_name": reg.ReplaceAllString(j.Chart_Name, ""), "chart_family": reg.ReplaceAllString(j.Chart_Family, ""), "chart_context": reg.ReplaceAllString(j.Chart_Context, ""), "chart_type": reg.ReplaceAllString(j.Chart_Type, ""), "units": reg.ReplaceAllString(j.Units, ""), "id": reg.ReplaceAllString(j.ID, ""), "name": reg.ReplaceAllString(j.Name, "")}
	j.Keyname = j.Labels["prefix"] + ":" + j.Labels["hostname"] + ":" + j.Labels["chart_family"] + ":" + j.Labels["chart_name"] + ":" + j.Labels["name"]
	j.Timestamp_Str = strconv.FormatInt(j.Timestamp*1000, 10)
	return nil
}

func (d *datapoint) Insert(redisClient rueidis.Client) error {
	rCmd := redisClient.B().TsAdd().Key(d.Keyname).Timestamp(d.Timestamp_Str).Value(d.Value).Labels()
	for key, label := range d.Labels {
		rCmd.Labels(key, label)
	}
	resp := redisClient.Do(context.Background(), rCmd.Build())
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
	maxDelay            time.Duration
	logConn             string
	redisBatch          int
	logger              = log.New(os.Stdout, "", log.LstdFlags)
	batch               atomic.Uint64
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
	flag.DurationVar(&redisDelay, "redis-delay", LookupEnvOrDuration("REDIS_DELAY", time.Millisecond*500), "Delay Redis TS.ADDs duration")
	flag.DurationVar(&maxDelay, "max-delay", LookupEnvOrDuration("MAX_DELAY", time.Millisecond*1000), "Max Delay Redis TS.ADDs duration")
	flag.IntVar(&redisBatch, "redis-batch", LookupEnvOrInt("REDIS_BATCH", 500), "Redis TS.ADD cmds max batch")
	flag.StringVar(&logConn, "connection-log", LookupEnvOrString("CONN_LOG", "standard"), "Show per connection detailed log - none, standard, detail")
	flag.Parse()
}

func server() {
	r, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:      []string{redisTimeSeriesHost},
		MaxFlushDelay:    redisDelay,
		DisableCache:     true,
		AlwaysPipelining: true,
	})
	if err != nil {
		log.Fatalf("Error while creating new connection to %s. error = %v", redisTimeSeriesHost, err)
	}
	defer r.Close()
	batch.Store(0)

	s, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("Error while trying to listen to %s. error = %v", listenAddress, err)
		return
	}
	logger.Printf("Configured redis delay is %s and logs %s...\n", redisDelay, logConn)
	logger.Printf("Listening at %s for netdata JSON inputs, and pushing RedisTimeSeries datapoints to %s...\n", listenAddress, redisTimeSeriesHost)
	//go ticker(r)
	for {
		// accept a connection
		c, err := s.Accept()
		if err != nil {
			logger.Println(err)
			continue
		}
		// handle the connection

		go handleServerConnection(c, r)
	}
}

// func ticker(redisClient rueidis.Client) {
// 	t := time.NewTicker(redisDelay)
// 	go func() {
// 		for ; ; <-t.C {
// 			if l := batch.Load(); l >= uint64(redisBatch) {
// 				respi := redisClient.Do(context.Background(), redisClient.B().TsIncrby().Key("netdataredistimeseriesrelay:counter").Value(float64(l)).Build())
// 				if err := respi.Error(); err != nil {
// 					logger.Printf("Error while trying to increase datapoint %d. error = %v\n", batch.Load(), err)
// 				} else {
// 					logger.Printf("Increased netdataredistimeseriesrelay:counter with %d...\n", l)
// 					batch.Store(0)
// 				}
// 			}
// 		}
// 	}()

// }

func handleServerConnection(c net.Conn, redisClient rueidis.Client) {
	defer c.Close()
	reader := bufio.NewScanner(c)
	reader.Split(bufio.ScanLines)

	if logConn != "none" {
		logger.Printf("Connection from %s\n", c.RemoteAddr())
	}
	for reader.Scan() {
		line := reader.Bytes()
		rcv := &datapointJSON{}
		err := json.Unmarshal(line, &rcv)
		if err != nil {
			log.Fatalf("Error while unmarshaling JSON. error = %v", err)
		}
		err = (*datapoint)(rcv).Insert(redisClient)
		if err != nil {
			log.Fatalf("Error while adding data points. error = %v", err)
		}
		if batch.Add(1); batch.Load() >= uint64(redisBatch) {
			l := batch.Swap(0)
			respi := redisClient.Do(context.Background(), redisClient.B().TsIncrby().Key("netdataredistimeseriesrelay:counter").Value(float64(l)).Build())
			if err := respi.Error(); err != nil {
				logger.Printf("Error while trying to increase datapoint, adding %d. error = %v\n", l, err)
				batch.Add(l)
			} else {
				logger.Printf("Increased netdataredistimeseriesrelay:counter with %d...\n", l)
			}
		}
	}
}

func main() {
	logger.Println("Starting netdata-redistimeseries-relay...")
	go server()
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()
	<-done
	logger.Println("Exiting...")
}
