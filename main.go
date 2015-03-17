package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/client"
)

type Config struct {
	Database  string
	Host      string
	Tags      int
	Values    int
	Rows      int
	BatchSize int
}

func main() {
	config := Config{}

	fs := flag.NewFlagSet("default", flag.ExitOnError)
	fs.IntVar(&config.Tags, "tags", 0, "number of tags to insert")
	fs.IntVar(&config.Values, "values", 1, "number of values to insert")
	fs.IntVar(&config.Rows, "rows", 1, "number of rows to insert")
	fs.IntVar(&config.BatchSize, "batch", 1, "number of rows to batch per write")
	fs.StringVar(&config.Database, "database", "test_load_database", "database to insert to")
	fs.StringVar(&config.Host, "host", "http://localhost:8086", "host to write to")
	fs.Parse(os.Args[1:])

	spew.Dump(config)

	u, e := url.Parse(config.Host)
	if e != nil {
		log.Printf("error parsing host %q: %s", config.Host, e)
		return
	}

	cl, e := client.NewClient(client.Config{URL: *u})
	if e != nil {
		log.Printf("error creating client: %s", e)
		return
	}

	// Create the database
	r, e := cl.Query(client.Query{Command: "create database " + config.Database})
	if e != nil && e.Error() != influxdb.ErrDatabaseExists.Error() {
		log.Printf("err creating database: %s", e)
		return
	}
	if r.Error() != nil && r.Error().Error() != influxdb.ErrDatabaseExists.Error() {
		log.Printf("err creating database: %s", r.Error())
		return
	}

	// Sleep 1 second to wait for default rp and db creation
	time.Sleep(time.Second)

	tags := Tags(config.Tags)

	var wg sync.WaitGroup
	wg.Add(config.Rows / config.BatchSize)

	var points []client.Point
	for i := 1; i <= config.Rows; i++ {

		p := client.Point{
			Name:      "p1",
			Timestamp: time.Now(),
			Tags:      randomTags(tags),
			Fields:    map[string]interface{}{"v1": randFloat(0, 1000)},
		}
		points = append(points, p)
		if len(points)%config.BatchSize == 0 {
			go func(points []client.Point, i int) {
				defer wg.Done()
				bp := client.BatchPoints{
					Database: config.Database,
					Points:   points,
				}

				log.Printf("Inserting %d rows batch %d\n", config.BatchSize, i/config.BatchSize)
				r, e := cl.Write(bp)
				if e != nil {
					log.Printf("err writing point: %s", e)
					panic("")
				}
				if r != nil && r.Error() != nil {
					log.Printf("err writing point: %s", r.Error())
					panic("")
				}
			}(points, i)
			// reset points
			points = nil
		}
	}
	wg.Wait()
}

func Tags(size int) []string {
	tagNames := []string{"region", "row", "rack", "slot", "host"}
	return tagNames[:size]
}

func randomTags(tags []string) map[string]string {
	mapTags := make(map[string]string)
	for _, t := range tags {
		switch t {
		case "region":
			r := []string{"uswest", "useast", "europe", "asia"}
			mapTags[t] = r[rand.Intn(len(r))]
		case "row":
			mapTags[t] = fmt.Sprintf("%d", rand.Intn(25))
		case "rack":
			mapTags[t] = fmt.Sprintf("%d", rand.Intn(50))
		case "slot":
			mapTags[t] = fmt.Sprintf("%d", rand.Intn(100))
		case "host":
			mapTags[t] = fmt.Sprintf("server%d", rand.Intn(1000))
		}
	}
	return mapTags
}

func randFloat(min, max float64) float64 {
	return rand.Float64()*(max-min) + min
}
