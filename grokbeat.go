package main

import (
	"sync"
	"github.com/spf13/viper"
	"os"
	"fmt"
	log "github.com/sirupsen/logrus"
	"path/filepath"
	"time"
	"github.com/hpcloud/tail"
	"crypto/md5"
	"grokbeat/libs/elastic"
	"github.com/vjeantet/grok"
)

var (
	wg sync.WaitGroup
	logfiles = make(map[string]bool)
	loglines = make(map[string][]string)
	grokedlines = make(map[string][]map[string]string)
	mutex = sync.RWMutex{}
	sys_params = make(map[string]string)
	elastic_params = make(map[string]string)
	grok_params = make(map[string]string)
	g, _ = grok.New()
	El = elastic.InitEl()
	index = ""
)

func getMD5Hash(text string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(text)))
}

func printlen(filename string) {
	timeout := time.After(1 * time.Minute)
	tick := time.Tick(1 * time.Second)
	for {
		select{
		case <-timeout:
			if len(grokedlines[filename]) >= 500 {
				status := elastic.BulkLogs(El, index, grokedlines[filename][:500])
				if status == 1 {
					log.Error("couldn't bulk")
					os.Exit(1)
				}
				mutex.Lock()
				grokedlines[filename] = grokedlines[filename][500:]
				mutex.Unlock()
			} else {
				l := len(grokedlines[filename])
				status := elastic.Send(El, index, grokedlines[filename][:l])
				if status == 1 {
					log.Error("couldn't send")
					os.Exit(1)
				}
				mutex.Lock()
				grokedlines[filename] = grokedlines[filename][l:]
				mutex.Unlock()
			}

		case <-tick:
			log.Infof("%s: %d", filename, len(grokedlines[filename]))
			if len(grokedlines[filename]) > 500 {
				status := elastic.BulkLogs(El, index, grokedlines[filename][:500])
				if status == 1 {
					log.Error("couldn't bulk")
					os.Exit(1)
				}
				mutex.Lock()
				grokedlines[filename] = grokedlines[filename][500:]
				mutex.Unlock()
			}
		}

	}
}

func logtail(filename string) {
	t, err := tail.TailFile(filename, tail.Config{Follow:true, ReOpen:true})

	go printlen(filename)

	if err != nil {
		panic(err)
	}

	for line := range t.Lines {
		logline := line.Text
		mutex.Lock()
		loglines[filename] = append(loglines[filename], logline)
		values, _ := g.Parse(grok_params["pattern"], logline)
		id := getMD5Hash(logline)
		parsedtime, _ := time.Parse("02/Jan/2006:15:04:05 -0700", values["timestamp"])
		values["@timestamp"] = parsedtime.Format(time.RFC3339)
		values["id"] = id
		values["filename"] = filename
		delete(values, "timestamp")
		grokedlines[filename] = append(grokedlines[filename], values)
		mutex.Unlock()
	}
	defer wg.Done()
}

func fileExists() {
	for f := range logfiles {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			delete(logfiles, f)
		}
	}
}

func inList(file string) (bool) {
	for f := range logfiles {
		if f == file {
			return false
		}
	}
	return true
}

func run(dir string) ([]string, error) {
	fileList := make([]string, 0)
	e := filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		fileList = append(fileList, path)
		return err
	})

	check(e)

	for _, file := range fileList {
		stat, _ := os.Stat(file)

		if !stat.IsDir() {
			if inList(file) {
				logfiles[file] = false
			}
		}
	}
	return fileList, nil
}

func check(e error) {
	if e != nil {
		log.Error(e)
		os.Exit(1)
	}
}

func main() {
	// read the config
	viper.SetConfigFile("./config.yml")
	err := viper.ReadInConfig()
	check(err)

	sys_params = viper.GetStringMapString("system")
	elastic_params = viper.GetStringMapString("elasticsearch")
	elastic_params["elastic_url"] = fmt.Sprintf("%s://%s:%s", elastic_params["protocol"], elastic_params["url"], elastic_params["port"])
	grok_params = viper.GetStringMapString("grok")

	// set the logger
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:01"
	customFormatter.FullTimestamp = true
	customFormatter.ForceColors = true
	log.SetFormatter(customFormatter)
	log.SetOutput(os.Stdout)

	dir := sys_params["dir"]

	check(err)

	El1, err := elastic.NewElastic(elastic_params["elastic_url"])
	El = El1
	check(err)

	info, code, err := elastic.Version(El)

	log.Infof("Elasticsearch returned with code %d and version %s", code, info.Version.Number)
	status := elastic.CreateIndex(El, elastic_params["index"])
	index = elastic_params["index"]


	switch status {
	case 0:
		log.Infof("index %s already exists", elastic_params["index"])
	case 1:
		log.Infof("created index %s", elastic_params["index"])
	case 2:
		log.Infof("couldn't create index %s", elastic_params["index"])
		os.Exit(1)
	}

	for {
		run(dir)

		for x := range logfiles {
			log.Info(x)
			if logfiles[x] == false {
				go logtail(x)
				wg.Add(1)
				logfiles[x] = true
			}
		}
		time.Sleep(30 * time.Second)
		log.Info("reload")
		fileExists()
	}
}
