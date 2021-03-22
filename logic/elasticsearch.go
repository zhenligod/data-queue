package logic

import (
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/m/config"
)

var (
	es      *elasticsearch.Client
	esIndex string
)

// EsLogic
type EsLogic struct {
	BaseLogic
	esConf EsConf
}

// EsConf es配置
type EsConf struct {
	IP       string
	Port     string
	Index    string
	Username string
	Passwd   string
	Es       *elasticsearch.Client
}

func init() {
	esConf := config.InitEs()
	config := elasticsearch.Config{}
	address := strings.Join([]string{
		"http",
		"//" + esConf.IP,
		esConf.Port,
	}, ":")
	config.Addresses = []string{address}
	config.Username = esConf.Username
	config.Password = esConf.Passwd
	esIndex = esConf.Index
	es, _ = elasticsearch.NewClient(config)
}

// Search search docs
func (e EsLogic) Search(body string, esIndex string) (*esapi.Response, error) {
	res, err := es.Search(
		es.Search.WithIndex(esIndex),
		es.Search.WithBody(strings.NewReader(body)),
		es.Search.WithPretty(),
	)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}

	return res, nil
}

// CreateDoc add doc with es client
func (e EsLogic) CreateDoc(id string, body string) (*esapi.Response, error) {
	res, err := es.Create(esIndex, id, strings.NewReader(body))

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}

	return res, nil
}

// UpdateDoc update doc with es client
func (e EsLogic) UpdateDoc(id string, body string) (*esapi.Response, error) {
	res, err := es.Update(esIndex, id, strings.NewReader(body))
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}

	return res, nil
}

// DeleteDoc delete doc with es client
func (e EsLogic) DeleteDoc(id string) (*esapi.Response, error) {
	res, err := es.Delete(esIndex, id)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}

	return res, nil
}

// GetDoc get sigle doc from es
func (e EsLogic) GetDoc(id string) (*esapi.Response, error) {
	res, err := es.Get(esIndex, id)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}

	return res, nil
}

// SQLDoc exec sql query
func (e EsLogic) SQLDoc(body string) (*esapi.Response, error) {
	res, err := es.SQL.Query(strings.NewReader(body))
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}

	return res, nil
}
