package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
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

type datapointJSON datapoint

type rediscmds struct {
	Commands  rueidis.Commands
	StartTime time.Time
	Client    rueidis.Client
	//Server    net.Conn
	Limit    int
	Mutex    sync.Mutex
	WG       sync.WaitGroup
	MaxDelay time.Duration
}

// func (d *datapoint) Prepare() *datapoint {
// 	reg, err := regexp.Compile("[^a-zA-Z0-9_./]+")
// 	if err != nil {
// 		log.Fatalf("Error while compiling regex. error = %v", err)
// 	}
// 	d.Labels = map[string]string{"prefix": reg.ReplaceAllString(d.Prefix, ""), "hostname": reg.ReplaceAllString(d.Hostname, ""), "chart_id": reg.ReplaceAllString(d.Chart_ID, ""), "chart_name": reg.ReplaceAllString(d.Chart_Name, ""), "chart_family": reg.ReplaceAllString(d.Chart_Family, ""), "chart_context": reg.ReplaceAllString(d.Chart_Context, ""), "chart_type": reg.ReplaceAllString(d.Chart_Type, ""), "units": reg.ReplaceAllString(d.Units, ""), "id": reg.ReplaceAllString(d.ID, ""), "name": reg.ReplaceAllString(d.Name, "")}
// 	d.Keyname = d.Labels["prefix"] + ":" + d.Labels["hostname"] + ":" + d.Labels["chart_family"] + ":" + d.Labels["chart_name"] + ":" + d.Labels["name"]
// 	d.Timestamp_Str = strconv.FormatInt(d.Timestamp*1000, 10)
// 	return d
// }

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

// func (d *datapoint) Insert(r rueidis.Client) error {
// 	rCmd := r.B().TsAdd().Key(d.Keyname).Timestamp(d.Timestamp_Str).Value(d.Value).Labels()
// 	for key, label := range d.Labels {
// 		rCmd.Labels(key, label)
// 	}
// 	resp := r.Do(context.Background(), rCmd.Build())
// 	if err := resp.Error(); err != nil {
// 		return err
// 	}
// 	return nil
// }

func (r *rediscmds) Connect() *rediscmds {
	redis, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:   []string{redisTimeSeriesHost},
		MaxFlushDelay: 20 * time.Microsecond,
		DisableCache:  true,
	})
	if err != nil {
		log.Fatalf("Error while creating new connection to %s. error = %v", redisTimeSeriesHost, err)
	}
	r.MaxDelay = maxDelay
	r.Client = redis
	return r
}

func (r *rediscmds) Write() *rediscmds {
	if r.Limit == 0 {
		return r
	}
	r.Mutex.Lock()
	r.WG.Add(1)
	defer r.Mutex.Unlock()
	defer r.WG.Done()
	//incrCmd := r.Client.B().TsIncrby().Key("netdataredistimeseriesrelay:counter").Value(float64(r.Limit))
	//r.Commands = append(r.Commands, incrCmd.Build())
	for _, resp := range r.Client.DoMulti(context.Background(), r.Commands...) {
		if err := resp.Error(); err != nil {
			log.Fatalf("Error while adding data points. error = %v", err)
		}
	}
	resp := r.Client.Do(context.Background(), r.Client.B().TsIncrby().Key("netdataredistimeseriesrelay:counter").Value(float64(r.Limit)).Build())
	if err := resp.Error(); err != nil {
		log.Fatalf("Error while increasing data points counter. error = %v", err)
	}
	if logConn != "none" {
		log.Printf("Processed %d entries, %s since last write.\n", r.Limit, time.Since(r.StartTime))
	}
	return r.init()
}

func (r *rediscmds) init() *rediscmds {
	r.Mutex.Lock()
	r.WG.Add(1)
	defer r.Mutex.Unlock()
	defer r.WG.Done()
	r.Commands = make(rueidis.Commands, 0, redisBatch)
	r.Limit = 0
	r.StartTime = time.Now()
	return r
}

// func (r *rediscmds) Log() {
// 	if len(r.Commands) == 0 {
// 		r.Commands = make(rueidis.Commands, 0, redisBatch)
// 		r.Limit = 0
// 	} else {
// 		log.Fatalf("Error while creating empty commands slice %v. error length = %d", r.Commands, len(r.Commands))
// 	}
// }

func (r *rediscmds) AddDatapoint(d *datapoint) *rediscmds {
	if r.Limit >= redisBatch || time.Since(r.StartTime) >= redisDelay {
		r.Write()
		r.WG.Wait()
		return r
	}
	r.Mutex.Lock()
	r.WG.Add(1)
	defer r.Mutex.Unlock()
	defer r.WG.Done()
	addCmd := r.Client.B().TsAdd().Key(d.Keyname).Timestamp(d.Timestamp_Str).Value(d.Value).Labels()
	for key, label := range d.Labels {
		addCmd.Labels(key, label)
	}
	r.Commands = append(r.Commands, addCmd.Build())
	r.Limit++
	return r
}

// Program option vars:
var (
	listenAddress       string
	redisTimeSeriesHost string
	redisDelay          time.Duration
	maxDelay            time.Duration
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
	flag.DurationVar(&redisDelay, "redis-delay", LookupEnvOrDuration("REDIS_DELAY", time.Millisecond*500), "Delay Redis TS.ADDs duration")
	flag.DurationVar(&maxDelay, "max-delay", LookupEnvOrDuration("MAX_DELAY", time.Millisecond*1000), "Max Delay Redis TS.ADDs duration")
	flag.IntVar(&redisBatch, "redis-batch", LookupEnvOrInt("REDIS_BATCH", 500), "Redis TS.ADD cmds max batch")
	flag.StringVar(&logConn, "connection-log", LookupEnvOrString("CONN_LOG", "standard"), "Show per connection detailed log - none, standard, detail")
	flag.Parse()
}

// func rtsClient() rueidis.Client {
// 	//r, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisTimeSeriesHost}, MaxFlushDelay: redisDelay})
// 	r, err := rueidis.NewClient(rueidis.ClientOption{
// 		InitAddress:   []string{redisTimeSeriesHost},
// 		MaxFlushDelay: 50 * time.Microsecond,
// 	})
// 	if err != nil {
// 		log.Fatalf("Error while creating new connection to %s. error = %v", redisTimeSeriesHost, err)
// 	}
// 	return r
// }

func server() {
	//r, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisTimeSeriesHost}, MaxFlushDelay: redisDelay})
	// r, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisTimeSeriesHost}})
	// if err != nil {
	// 	log.Fatalf("Error while creating new connection to %s. error = %v", redisTimeSeriesHost, err)
	// }
	r := rediscmds{}
	r.Connect().init()
	defer r.Client.Close()
	s, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("Error while trying to listen to %s. error = %v", listenAddress, err)
		return
	}
	log.Printf("Configured redis delay is %s, max delay is %s, netdata JSON connection batch size is %d, and logs %s...\n", redisDelay, maxDelay, redisBatch, logConn)
	log.Printf("Listening at %s for netdata JSON inputs, and pushing RedisTimeSeries datapoints to %s...\n", listenAddress, redisTimeSeriesHost)
	go ticker(&r)
	for {
		// accept a connection
		c, err := s.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		//r.Server = c
		// handle the connection
		go handleServerConnection(&r, c)
	}
}

func ticker(r *rediscmds) {
	t := time.NewTicker(r.MaxDelay)
	go func() {
		for ; ; <-t.C {
			if logConn == "detail" {
				log.Println("Ticker write!")
			}
			r.Write()
			r.WG.Wait()
		}
	}()

}

func handleServerConnection(r *rediscmds, c net.Conn) {
	defer c.Close()
	//defer r.Client.Close()
	//c.SetDeadline(time.Now().Add(redisDelay))
	//tnow := time.Now()
	reader := bufio.NewScanner(c)
	//reader.Split(bufio.ScanLines)

	//rem := c.RemoteAddr().String()
	//cmds := make(rueidis.Commands, 0, redisBatch)
	if logConn == "detail" {
		log.Printf("Connection from %s, received %d bytes\n", c.RemoteAddr(), len(reader.Bytes()))
	}
	for reader.Scan() {
		line := reader.Bytes()
		rcv := &datapointJSON{}
		err := json.Unmarshal(line, &rcv)
		if err != nil {
			log.Fatalf("Error while unmarshaling JSON. error = %v", err)
		}
		//rcv.Prepare()
		r.AddDatapoint((*datapoint)(rcv))
		r.WG.Wait()
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
