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
	"grokbeat_noclass/libs/elastic"
	"github.com/vjeantet/grok"
	"strings"
	"strconv"
	"io"
	"bytes"
	_ "net/http/pprof"
	"net/http"
)

var (
	logfiles = make(map[string]bool)
	filelines = make(map[string]int)
	backuplines = make(map[string]int)
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
	backuppedlines = make(map[string]int)
	logs = elastic.New()
	//indices = make(map[string]bool)
)

func writeStatus() {
	for {
		file, _ := os.Create("status.yml")
		defer file.Close()
		fmt.Fprintln(file, "status:")
		mutex.Lock()
		for key, value := range filelines {
			if backuppedlines[key] > value {
				value = backuppedlines[key]
			}
			fmt.Fprintf(file, "  %s: %d\n", key, value)
		}
		mutex.Unlock()
		time.Sleep(1 * time.Minute)
	}
}

func getMD5Hash(text string) string {
	hash := fmt.Sprintf("%x", md5.Sum([]byte(text)))
	return hash
}

func countLines(filename string) (end int ) {
	file, err := os.Open(filename)
	check(err)

	buf := make([]byte, 1024)
	lines := 0

	for {
		readBytes, err := file.Read(buf)

		if err != nil {
			if readBytes == 0 && err == io.EOF {
				err = nil
			}
			return lines
		}
		lines += bytes.Count(buf[:readBytes], []byte{'\n'})
	}
	return lines
}


func logtail(filename string, end int) {
	t, err := tail.TailFile(filename, tail.Config{Follow:true, ReOpen:true, Logger: tail.DiscardingLogger,})

	if err != nil {
		panic(err)
	}

	for line := range t.Lines {
		mutex.Lock()
		linenr := filelines[filename]
		backline := backuppedlines[filename]
		mutex.Unlock()
		if backline < linenr {
			logline := line.Text
			id := getMD5Hash(fmt.Sprintf("%s-%i", logline, linenr))
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

			parsedtime, _ := time.Parse("02/Jan/2006:15:04:05 -0700", values["timestamp"])
			newindex := fmt.Sprintf("%s-%04d.%02d.%02d", index, parsedtime.Year(), parsedtime.Month(), parsedtime.Day())
			found := elastic.SearchID(El, newindex, elastic_params["doctype"], id)
			if found == false {
				mutex.Lock()
				//indexExists := indices[newindex]
				docType := elastic_params["doctype"]
				mutex.Unlock()
				values["@timestamp"] = parsedtime.Format(time.RFC3339)
				values["id"] = id
				values["filename"] = filename
				values["type"] = docType
				values["_index"] = newindex
				delete(values, "timestamp")
				logs.Add(values)
				values = make(map[string]string)
				linenr++
				mutex.Lock()
				filelines[filename] = linenr
				mutex.Unlock()
			} else {
				log.Warnf("Document with ID %s already exists", id)
			}

			if strings.HasPrefix(filename, "backup/backup_") {
				mutex.Lock()
				end := backuplines[filename]
				mutex.Unlock()
				if end == linenr {
					os.Remove(filename)
				}
			}
		} else {
			linenr++
			mutex.Lock()
			filelines[filename] = linenr
			mutex.Unlock()
		}
	}
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
				mutex.Lock()
				logfiles[file] = false
				if strings.Contains(file, "backup/backup_") {
					backuplines[file] = countLines(file)
				}
				mutex.Unlock()
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

	go func() {
		log.Info(http.ListenAndServe("localhost:6060", nil))
	}()

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

	limit := int64(1000)
	if _, ok := elastic_params["bulklimit"]; ok {
		limit, _ = strconv.ParseInt(elastic_params["bulklimit"], 0, 32)
	}

	if _, err := os.Stat("./status.yml"); err == nil {
		viper.SetConfigFile("./status.yml")
		err := viper.ReadInConfig()
		check(err)
		stats := viper.GetStringMap("status")
		for key, value := range stats {
			backuppedlines[key] = value.(int)
			fmt.Printf("%s: %d\n", key, value.(int))
		}
	}

	go func(){
		elastic.Tick(int(limit), El, logs)
	}()

	go writeStatus()

	for {
		run(dir)
		run(backup)
		ifExists()

		for x := range logfiles {
			for end := range pattern_index {
				if logfiles[x] == false && strings.HasSuffix(x, pattern_index[end]){
					go logtail(x, end)
					logfiles[x] = true
				}

				if strings.HasSuffix(x, pattern_index[end]) {
					mutex.Lock()
					if _, ok := filelines[x]; !ok {
						filelines[x] = 0
						backuppedlines[x] = 0
					}
					mutex.Unlock()
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
								mutex.Lock()
								filelines[x] = 0
								backuppedlines[x] = 0
								mutex.Unlock()
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
		//fileExists()
	}
}
