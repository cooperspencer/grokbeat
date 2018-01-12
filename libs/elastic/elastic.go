package elastic

import (
	"net/http"
	"crypto/tls"
	"gopkg.in/olivere/elastic.v5"
	"context"
	"fmt"
	"encoding/json"
	"time"
)

type Elastic struct {
	ElasticSearch *elastic.Client
	url 		  string
}

type Search struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Found   bool   `json:"found"`
	Source  struct {
		Timestamp       time.Time `json:"@timestamp"`
		BASE10NUM       string    `json:"BASE10NUM"`
		COMMONAPACHELOG string    `json:"COMMONAPACHELOG"`
		EMAILADDRESS    string    `json:"EMAILADDRESS"`
		EMAILLOCALPART  string    `json:"EMAILLOCALPART"`
		HOSTNAME        string    `json:"HOSTNAME"`
		HOUR            string    `json:"HOUR"`
		INT             string    `json:"INT"`
		IP              string    `json:"IP"`
		IPV4            string    `json:"IPV4"`
		IPV6            string    `json:"IPV6"`
		MINUTE          string    `json:"MINUTE"`
		MONTH           string    `json:"MONTH"`
		MONTHDAY        string    `json:"MONTHDAY"`
		SECOND          string    `json:"SECOND"`
		SPACE           string    `json:"SPACE"`
		TIME            string    `json:"TIME"`
		USER            string    `json:"USER"`
		USERNAME        string    `json:"USERNAME"`
		YEAR            string    `json:"YEAR"`
		Auth            string    `json:"auth"`
		Bytes           string    `json:"bytes"`
		Clientip        string    `json:"clientip"`
		Filename        string    `json:"filename"`
		Httpversion     string    `json:"httpversion"`
		ID              string    `json:"id"`
		Ident           string    `json:"ident"`
		Rawrequest      string    `json:"rawrequest"`
		Request         string    `json:"request"`
		Response        string    `json:"response"`
		Type            string    `json:"type"`
		Usertest        string    `json:"usertest"`
		Verb            string    `json:"verb"`
	} `json:"_source"`
}

func InitEl() (ela *Elastic) {
	client, _ := elastic.NewClient()
	return &Elastic{client, ""}
}

func NewElastic(elasticurl string) (ela *Elastic, err error){
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpclient := &http.Client{Transport: tr}
	client, err := elastic.NewClient(
		elastic.SetHttpClient(httpclient),
		elastic.SetURL(elasticurl),
		elastic.SetScheme("https"),
		elastic.SetSniff(false),
	)

	return &Elastic{client, elasticurl}, err
}

func Version(elastic *Elastic) (info *elastic.PingResult , code int, err error) {
	info, code, err = elastic.ElasticSearch.Ping(elastic.url).Do(context.Background())
	return info, code, err
}

func CreateIndex(elastic *Elastic, doctype, index string) (status int) {
	exists, _ := elastic.ElasticSearch.IndexExists(index).Do(context.Background())
	if exists {
		return 0
	} else{
		mapping := `
{
	"mappings": {
		"` + doctype + `": {
			"properties": {
				"@timestamp": {
					"type": "date"
				}
			}
		}
	}
}`
		createindex, _ := elastic.ElasticSearch.CreateIndex(index).Body(mapping).Do(context.Background())

		if createindex.Acknowledged {
			return 1
		} else {
			return 2
		}
	}
}

func BulkLogs(elasticsearch *Elastic, index string, loglines []map[string]string) (status int) {
	bulkRequest := elasticsearch.ElasticSearch.Bulk()
	for _, logline := range loglines {
		if logline != nil {
			index = logline["_index"]
			delete(logline, "_index")
			req := elastic.NewBulkIndexRequest().Index(index).Type(logline["type"]).Id(logline["id"]).Doc(logline)
			bulkRequest = bulkRequest.Add(req)
		}
	}
	bulkResponse, err := bulkRequest.Do(context.Background())
	if err != nil {
		return 1
	}
	bulkResponse.Created()
	return 0
}

func Send(elasticsearch *Elastic, index string, loglines []map[string]string) (status int) {
	for _, logline := range loglines {
		index = logline["_index"]
		delete(logline, "_index")
		_, err := elasticsearch.ElasticSearch.Index().Index(index).Type(logline["type"]).Id(logline["id"]).BodyJson(logline).Do(context.Background())

		if err != nil {
			return 1
		}
	}
	return 0
}

func SearchID(elasticsearch *Elastic, index, doctype, id string) (status bool) {
	url := fmt.Sprintf("%s/%s/%s/%s", elasticsearch.url, index, doctype, id)
	found := Search{}
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("threw an error")
		return false
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&found)
	return found.Found
}
