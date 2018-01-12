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
	"strings"
	"strconv"
)

var (
	wg sync.WaitGroup
	logfiles = make(map[string]bool)
	loglines = make(map[string][]string)
	grokedlines = make(map[string][]map[string]string)
	mutex = sync.RWMutex{}
	sys_params = make(map[string]string)
	elastic_params = make(map[string]string)
	grok_params = make(map[string]interface{})
	patterns = make(map[string]interface{})
	pattern_index = []string{}
	g, _ = grok.New()
	El = elastic.InitEl()
	index = ""
	backup = "./backup"
	toDelete = make(map[string]int)
	//indices = make(map[string]bool)
)

func getMD5Hash(text string) string {
	hash := fmt.Sprintf("%x", md5.Sum([]byte(text)))
	return hash
}

func printlen(filename string) {
	tick := time.Tick(100 * time.Millisecond)
	for {
		select{
		case <-tick:
			mutex.Lock()
			lenGrokedLines := len(grokedlines[filename])
			bulklimit, _ := strconv.ParseInt(elastic_params["bulklimit"], 0, 32)
			mutex.Unlock()

			if lenGrokedLines != 0 {
				log.Infof("%s: %d", filename, lenGrokedLines)
				if lenGrokedLines > int(bulklimit) {
					mutex.Lock()
					lines := grokedlines[filename][:int(bulklimit)]
					mutex.Unlock()
					status := elastic.BulkLogs(El, index, lines)
					for status == 1 {
						log.Error("elasticsearch seems down, trying again in 5 seconds")
						time.Sleep(5)
						status = elastic.BulkLogs(El, index, lines)
					}
					mutex.Lock()
					grokedlines[filename] = grokedlines[filename][int(bulklimit):]
					mutex.Unlock()
				} else {
					mutex.Lock()
					l := len(grokedlines[filename])
					mutex.Unlock()
					mutex.Lock()
					lines := grokedlines[filename][:l]
					mutex.Unlock()
					status := elastic.Send(El, index, lines)
					if status == 1 {
						log.Error("couldn't send")
						os.Exit(1)
					}
					mutex.Lock()
					grokedlines[filename] = grokedlines[filename][l:]
					mutex.Unlock()
				}
			}
			if lenGrokedLines == 0 && strings.HasPrefix(filename, "backup/backup_") {
				mutex.Lock()
				toDelete[filename] += 1
				count := toDelete[filename]
				mutex.Unlock()
				if count >= 1000 {
					log.Warnf("Deleting %s", filename)
					os.Remove(filename)
					mutex.Lock()
					delete(toDelete, filename)
					delete(grokedlines, filename)
					mutex.Unlock()
					return
				}
			} else {
				mutex.Lock()
				toDelete[filename] = 0
				mutex.Unlock()
			}
		}

	}
}

func logtail(filename string, end int) {
	t, err := tail.TailFile(filename, tail.Config{Follow:true, ReOpen:true})

	go printlen(filename)

	if err != nil {
		panic(err)
	}

	for line := range t.Lines {
		logline := line.Text
		mutex.Lock()
		loglines[filename] = append(loglines[filename], logline)
		mutex.Unlock()
		pattern := patterns[pattern_index[end]].([]interface{})
		values := make(map[string]string)
		len_keys := 0;
		for i := range pattern {
			if len_keys == 0 {
				values, _ = g.Parse(pattern[i].(string), logline)
				for range values {
					len_keys++;
				}
			} else {
				break
			}
		}
		if len_keys == 0 {
			log.Errorf("%s: The parser doesn't fit", filename);
		}

		id := getMD5Hash(logline)
		found := elastic.SearchID(El, elastic_params["index"], elastic_params["doctype"], id)
		if found == false {
			parsedtime, _ := time.Parse("02/Jan/2006:15:04:05 -0700", values["timestamp"])
			newindex := fmt.Sprintf("%s-%d.%d.%d", index, parsedtime.Year(), parsedtime.Month(), parsedtime.Day())

			mutex.Lock()
			//indexExists := indices[newindex]
			docType := elastic_params["doctype"]
			mutex.Unlock()
			/*
			if !indexExists {
				createIndex(newindex, docType)
				mutex.Lock()
				indices[newindex] = true
				mutex.Unlock()
			}
			*/
			values["@timestamp"] = parsedtime.Format(time.RFC3339)
			values["id"] = id
			values["filename"] = filename
			values["type"] = docType
			values["_index"] = newindex
			delete(values, "timestamp")
			mutex.Lock()
			grokedlines[filename] = append(grokedlines[filename], values)
			mutex.Unlock()
		}
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

func ifExists() {
	for file := range(logfiles) {

		if _, err := os.Stat(file); os.IsNotExist(err) {
			delete(logfiles, file)
		}

	}
}

func createIndex(index, doctype string) {
	status := elastic.CreateIndex(El, doctype, index)
	switch status {
	case 1:
		log.Infof("created index %s", index)
	case 2:
		log.Infof("couldn't create index %s", index)
		os.Exit(1)
	}
}

func check(e error) {
	if e != nil {
		log.Error(e)
		os.Exit(1)
	}
}

func getSize(size int64) (filesize map[string]float64) {
	var kilobytes float64
	var megabytes float64
	var gigabytes float64
	var terabytes float64

	kilobytes = float64(size / 1024)
	megabytes = float64(kilobytes / 1024)
	gigabytes = float64(megabytes / 1024)
	terabytes = float64(gigabytes / 1024)

	filesize = make(map[string]float64)

	filesize["K"] = kilobytes
	filesize["M"] = megabytes
	filesize["G"] = gigabytes
	filesize["T"] = terabytes

	return filesize
}

func main() {
	// read the config
	viper.SetConfigFile("./config.yml")
	err := viper.ReadInConfig()
	check(err)

	sys_params = viper.GetStringMapString("system")
	elastic_params = viper.GetStringMapString("elasticsearch")
	elastic_params["elastic_url"] = fmt.Sprintf("%s://%s:%s", elastic_params["protocol"], elastic_params["url"], elastic_params["port"])
	grok_params = viper.GetStringMap("grok")

	patterns = grok_params["patterns"].(map[string]interface{})

	for key := range patterns {
		pattern_index = append(pattern_index, key)
	}

	// set the logger
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:01"
	customFormatter.FullTimestamp = true
	customFormatter.ForceColors = true
	log.SetFormatter(customFormatter)
	log.SetOutput(os.Stdout)

	dir := sys_params["dir"]
	size := sys_params["filesize"]
	bytes := size[len(sys_params["filesize"])-1:]
	maxsize, _ := strconv.ParseFloat(size[:len(sys_params["filesize"])-1], 64)

	check(err)

	El1, err := elastic.NewElastic(elastic_params["elastic_url"])
	El = El1
	check(err)

	info, code, err := elastic.Version(El)

	log.Infof("Elasticsearch returned with code %d and version %s", code, info.Version.Number)
	index = elastic_params["index"]

	for {
		run(dir)
		run(backup)
		ifExists()

		for x := range logfiles {
			for end := range pattern_index {
				if logfiles[x] == false && strings.HasSuffix(x, pattern_index[end]){
					go logtail(x, end)
					wg.Add(1)
					logfiles[x] = true
				}
				if strings.HasSuffix(x, pattern_index[end]) {
					file, _ := os.Open(x)
					defer file.Close()
					stat, _ := file.Stat()
					filesize := getSize(stat.Size())
					log.Infof("%s: Filesize %f", x, filesize[bytes])
					if maxsize < filesize[bytes] && !strings.Contains(x, "backup/backup_") {
						log.Warnf("backuping %s", x)
						splittedfile := strings.Split(x, "/")
						backupfile := "backup_" + splittedfile[len(splittedfile)-1]
						backupf := backup + "/" + backupfile
						tries := 1

						for {
							if _, err := os.Stat(backupf); os.IsNotExist(err) {
								err := os.Rename(x, backupf)
								check(err)
								break
							} else {
								backupfile = fmt.Sprintf("backup_%d_%s", tries, splittedfile[len(splittedfile)-1])
								backupf = backup + "/" + backupfile
								log.Warn("File already exists. Trying next one!")
							}
						}

					}
				}
			}
		}
		time.Sleep(30 * time.Second)
		log.Info("reload")
		fileExists()
	}
}
