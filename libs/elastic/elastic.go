package elastic

import (
	"net/http"
	"crypto/tls"
	"gopkg.in/olivere/elastic.v5"
	"context"
)

type Elastic struct {
	ElasticSearch *elastic.Client
	url 		  string
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

func CreateIndex(elastic *Elastic, index string) (status int) {
	exists, _ := elastic.ElasticSearch.IndexExists(index).Do(context.Background())
	if exists {
		return 0
	} else{
		mapping := `
{
	"mappings": {
		"portal": {
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
			req := elastic.NewBulkIndexRequest().Index(index).Type("portal").Id(logline["id"]).Doc(logline)
			bulkRequest = bulkRequest.Add(req)
		}
	}
	bulkResponse, err := bulkRequest.Do(context.Background())
	if err != nil {
		panic(err)
		return 1
	}
	bulkResponse.Created()
	return 0
}

func Send(elasticsearch *Elastic, index string, loglines []map[string]string) (status int) {
	for _, logline := range loglines {
		_, err := elasticsearch.ElasticSearch.Index().Index(index).Type("portal").Id(logline["id"]).BodyJson(logline).Do(context.Background())

		if err != nil {
			return 1
		}
	}
	return 0
}