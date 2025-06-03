package services

import (
	"context"
	//"encoding/json"
	"fmt"
	"log"

	"bus-location-system/config"
	//"bus-location-system/models"

	"github.com/olivere/elastic/v7"
)

type ElasticsearchService struct {
	client *elastic.Client
	ctx    context.Context
}

// ElasticsearchService 인스턴스 생성
func NewElasticsearchService() (*ElasticsearchService, error) {
	var client *elastic.Client
	var err error

	// 인증 정보가 있는 경우
	if config.AppConfig.ESUsername != "" && config.AppConfig.ESPassword != "" {
		client, err = elastic.NewClient(
			elastic.SetURL(config.AppConfig.ESAddr),
			elastic.SetBasicAuth(config.AppConfig.ESUsername, config.AppConfig.ESPassword),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
		)
	} else {
		client, err = elastic.NewClient(
			elastic.SetURL(config.AppConfig.ESAddr),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("Elasticsearch 클라이언트 생성 실패: %v", err)
	}

	ctx := context.Background()

	// 연결 테스트
	info, code, err := client.Ping(config.AppConfig.ESAddr).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("Elasticsearch 연결 실패: %v", err)
	}

	log.Printf("Elasticsearch 연결 성공 - Code: %d, Version: %s", code, info.Version.Number)

	esService := &ElasticsearchService{
		client: client,
		ctx:    ctx,
	}

	// 인덱스 초기화
	//if err := esService.initializeIndex(); err != nil {
	//	return nil, fmt.Errorf("인덱스 초기화 실패: %v", err)
	//}

	return esService, nil
}
