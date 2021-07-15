package main

import (
	"encoding/json"
	"io/ioutil"
	"fmt"
	"os"

	"flag"
	"strings"

	"log"
	"strconv"
	"io"
	"time"
	"os/user"
	"path/filepath"

	//"bufio"
	"sync"
	//"unicode"
	//"errors"

	"context"
	"github.com/go-redis/redis/v8"
)


/** logging **/
type logWriter struct {
    writer io.Writer
	timeFormat string
	user string
	hostname string
}

func (w logWriter) Write(b []byte) (n int, err error) {
	return w.writer.Write([]byte(time.Now().Format(w.timeFormat) + "\t" + "ptail@" + w.hostname + "\t" + w.user + "\t" + strconv.Itoa(os.Getpid()) + "\t" + string(b)))
}
func index(slice *[]string, x string) int {
    for i, n := range *slice {
        if x == n {
            return i
        }
    }
    return -1
}

// slice helper
func removeByValue(slice *[]string, value string) *[]string {
	//s[i] = s[len(s)-1]
	idx := index(slice, value)
	if idx < 0 {
		return slice
	} else {
		//return append(slice[:idx], slice[idx+1:]...) // keep order?
		(*slice)[idx] = (*slice)[len(*slice)-1]
		*slice = (*slice)[:len(*slice)-1]
		return slice
	}
}

/** tailer **/

func readerRoutine(group string, consumer string, keys *[]string) chan bool {
	ch := make(chan bool)
	go func() {

		for {
			select {
				case <-ch:
					return
				default:
					initialLen := len(*keys)
					keysWithPos := make([]string, len(*keys))
					copy(keysWithPos, *keys)

					for i := 1; i <= initialLen; i++ {
						keysWithPos = append(keysWithPos, ">")
					}

					res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
						Group: group,
						Consumer: consumer,
						Streams: keysWithPos,
						Count: 1000,
						Block: (time.Duration(1) * time.Second),
						NoAck: true,
					}).Result()
					
					if err == redis.Nil {
						// no new data
					} else if (err != nil) {
						if strings.HasPrefix(err.Error(), "NOGROUP") {
							globber(keyGlobs, keys)
						} else {
							logger.Println("ERROR:", err)
						}
					} else {
					
						if len(res) > 0 {
							for _,data := range res {
								if len(data.Messages) > 0 {
									for _,msg := range data.Messages {
										//object := make(map[string]interface{})
										//object["data"]= msg.Values
										object := msg.Values
										object["__stream"] = data.Stream
										object["__msgid"] = msg.ID
										jsonString, _ := json.Marshal(object)
										fmt.Println(string(jsonString))
										//fmt.Println("stream:",data.Stream,",ID:",msg.ID,",Values:",msg.Values)
									}
								}
							} 
						}
					}
			}
		}
		
		// ?? 
	}()
	return ch
}

/** argparse **/
type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, ",")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var keyGlobs arrayFlags
var sourceFile bool



// from https://stackoverflow.com/questions/35809252/check-if-flag-was-provided-in-go
// thanks to Markus Heukelom
func isFlagPassed(name string) bool {
    found := false
    flag.Visit(func(f *flag.Flag) {
        if f.Name == name {
            found = true
        }
    })
    return found
}

/** additional helper functions **/

/**
	search keys 
**/
func globber(input arrayFlags, keys *[]string) {
	//keys := make([]string, 0)
	for _,keyGlob := range input {
		val, err := rdb.Keys(ctx, keyGlob).Result()
		if err != nil {
			panic(err)
		}

		if len(val) > 0 {
			
			/// add new keys

			for _,globKeyValue := range val {		// iterate over values from glob search
				exists := false
				for _,keyValue := range *keys {		// iterate over current keys
					if globKeyValue == keyValue { 
						exists = true
					}
				}
				
				if !exists {	// we don't have that key yet, so add it to the list
					*keys = append(*keys, globKeyValue)
				}
			}

			// remove dropped keys

			for _,keyValue := range *keys {			// iterate over current keys
				exists := false
				for _,globKeyValue := range val {	// iterate over values from glob search
					if globKeyValue == keyValue { 
						exists = true
					}
				}

				if !exists {
					// remove from keys
					keys = removeByValue(keys, keyValue)
				}
			}

		}
	}
}

/**
	register xgroups
**/
func createGroups(keys []string, groupName string) {
	for _,key := range keys {

		// now we ask if this stream already has some groups, if not - we create one
		// !!! this should search for specific groupName !!!
		res, err := rdb.XInfoGroups(ctx, key).Result()
		if err == nil && len(res) == 0 {
			
			readPos := "0" // this can also be $ for last
			_, createErr := rdb.XGroupCreate(ctx, key, groupName, readPos).Result()
			if createErr != nil {
				//logger.Println("ERR:", err)
				// BUSYGROUP Consumer Group name already exists
			}
		} else if (err != nil) {
			fmt.Println(err)
		}
	}
}

var ctx = context.Background()
var rdb redis.Client

var keys []string = make([]string, 0)

var logger *log.Logger

func main() {
	flag.Var(&keyGlobs, "key", "key (or glob pattern) to tail (can be used multiple times); (default: '*' - all keys")
	logToFile := flag.String("log", "", "enable logging. \"-\" for stdout, filename otherwise")
	redisHost := flag.String("host", "localhost", "redis host name (default: localhost)")
	redisPort := flag.Int("port", 6379, "redis port (default: 6379)")
	redisGroup := flag.String("group", "group", "group name for redis xreadgroup command (default: group)")
	redisConsumer := flag.String("consumer", "consumer", "consumer name for redis xreadgroup command (default: consumer)")
	glob := flag.Int64("glob", 0, "interval in seconds for re-running glob search (default is 0 - disabled; only initially found files will be monitored). Will be auto-set to 1 if globbing detected.")
	wait := flag.Bool("wait", true, "wait for keys to appear, don't exit program if monitored (and actually existing) key count is 0")
	flag.Parse()

    //sourceFile = *sourceFileFlag

	/**
		set up logging
	**/

	if *logToFile == "" {
		logger = log.New(ioutil.Discard, "", 0)
	} else { // enable logging
		// get user info for log
		userobj, err := user.Current()
		usern := "-"
		if err != nil {
			log.Println("failed getting current user")
		}
		usern = userobj.Username

		// get host info for log
		hostname, err := os.Hostname()
		if err != nil {
			log.Println("failed getting machine hostname")
			hostname = "-"
		}

		if *logToFile == "-" { // to stdout
			logger = log.New(os.Stdout, "", 0)
			logger.SetFlags(0)
			logger.SetOutput(logWriter{writer: logger.Writer(), timeFormat: "2006-01-02T15:04:05.999Z07:00", user: usern, hostname: hostname})
		} else { // to file
			// get binary path
			logpath := *logToFile
			dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
			if err != nil {
				log.Println("couldn't get binary path - logfile path is relative to exec dir")
			} else {
				logpath = dir + "/" + *logToFile
			}

			f, err := os.OpenFile(logpath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
			if err != nil {
				log.Println("opening file " + *logToFile + " failed, writing log to stdout")
			} else {
				defer f.Close()
				logger = log.New(f, "", 0)
				logger.SetOutput(logWriter{writer: logger.Writer(), timeFormat: "2006-01-02T15:04:05.999Z", user: usern, hostname: hostname})
			}
		}
	}

	if len(keyGlobs) == 0 {
		//logger.Println("no keys specified, exiting")
		keyGlobs = append(keyGlobs, "*")
		//os.Exit(1)
	}


	globSet := false
	for _,key := range keyGlobs {
		if strings.Contains(key, "*") || strings.Contains(key, "?") || strings.Contains(key, "[") || strings.Contains(key, "]") {
			globSet = true
		}
	}
	
	if globSet && !isFlagPassed("glob") {
		*glob = 1
		logger.Println("detected wildcard pattern, overriding -glob value to 1; set 0 excplicitly to disable")
	}

	logger.Println("starting with conf values - TODO TODO TODO")

	rdb = *redis.NewClient(&redis.Options{
        Addr:     *redisHost + ":" + strconv.Itoa(*redisPort),
        Password: "", // no password set
        DB:       0,  // use default DB
    })

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		logger.Println("failed to connect to redis. error: ", err)
		os.Exit(2)
	}

	globber(keyGlobs, &keys)

	createGroups(keys, *redisGroup)

	var wg sync.WaitGroup

	readerRoutine(*redisGroup, *redisConsumer, &keys)

	wg.Add(1)

	if *wait {
		wg.Add(1)
	}
	// start perodical globbing
	
	if *glob > 0 {
		go func() {
			for range time.Tick(time.Duration(*glob) * time.Second) {
				globber(keyGlobs, &keys)
				createGroups(keys, *redisGroup)
			}
		}()
	}

	// wait for... santa?
	wg.Wait()

}
