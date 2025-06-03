package services

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"bus-location-system/config"
	"bus-location-system/models"
	"bus-location-system/utils"

	"github.com/olivere/elastic/v7"
)

type ElasticsearchService struct {
	client       *elastic.Client
	ctx          context.Context
	redisService *RedisService
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

	// 통합 인덱스 초기화
	if err := esService.initializeIndex(); err != nil {
		return nil, fmt.Errorf("인덱스 초기화 실패: %v", err)
	}

	return esService, nil
}

// Redis 서비스 설정
func (es *ElasticsearchService) SetRedisService(redisService *RedisService) {
	es.redisService = redisService
}

// 인덱스 초기화
func (es *ElasticsearchService) initializeIndex() error {
	indexName := config.AppConfig.ESUnifiedIndex

	exists, err := es.client.IndexExists(indexName).Do(es.ctx)
	if err != nil {
		return fmt.Errorf("인덱스 존재 확인 실패: %v", err)
	}

	if !exists {
		mapping := `{
			"mappings": {
				"properties": {
					"routeId": {"type": "keyword"},
					"routeName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
					"timestamp": {"type": "date", "format": "epoch_second"},
					"vehicleId": {"type": "keyword"},
					"vehicleNo": {"type": "keyword"},
					"operationStatus": {"type": "keyword"},
					"location": {"type": "geo_point"},
					"nodeOrd": {"type": "integer"},
					"nodeName": {"type": "text"},
					"stationSeq": {"type": "integer"},
					"crowded": {"type": "integer"},
					"remainSeatCnt": {"type": "integer"},
					"stateCd": {"type": "integer"}
				}
			}
		}`

		_, err := es.client.CreateIndex(indexName).BodyString(mapping).Do(es.ctx)
		if err != nil {
			return fmt.Errorf("인덱스 생성 실패: %v", err)
		}

		log.Printf("Elasticsearch 인덱스 '%s' 생성 완료", indexName)
	}

	return nil
}

// === 메인 동기화 함수 - 이벤트 기반 처리 ===
func (es *ElasticsearchService) SyncUnifiedDataFromRedis(redisService *RedisService) error {
	utils.LogInfo("📡 ES 동기화 시작")

	totalDocs := 0
	for _, routeID := range config.AppConfig.RouteIDs {
		// 변경된 버스 감지
		changedVehicles := es.detectChangedVehicles(redisService, routeID)
		if len(changedVehicles) == 0 {
			continue
		}

		// 통합 데이터 생성 및 전송
		unifiedData := es.createUnifiedData(redisService, routeID, changedVehicles)
		if len(unifiedData) > 0 {
			// 각 노선별로 즉시 ES 전송 (이벤트 기반)
			if err := es.sendToElasticsearch(unifiedData); err != nil {
				utils.LogError("노선 %s ES 전송 실패: %v", routeID, err)
				continue
			}
			totalDocs += len(unifiedData)
		}
	}

	if totalDocs > 0 {
		utils.LogInfo("✅ ES 동기화 완료: %d개 문서 처리", totalDocs)
	}

	return nil
}

// === 변경 감지 최적화 - 병렬 처리 ===
func (es *ElasticsearchService) detectChangedVehicles(redisService *RedisService, routeID string) []string {
	// 활성 버스 목록 조회
	vehicles, err := redisService.GetActiveBuses(routeID)
	if err != nil || len(vehicles) == 0 {
		utils.LogDebug("노선 %s: 활성 버스 없음", routeID)
		return []string{}
	}

	type vehicleCheck struct {
		vehicleNo string
		changed   bool
	}

	checkChan := make(chan vehicleCheck, len(vehicles))
	var wg sync.WaitGroup

	// 각 차량을 병렬로 체크
	for _, vehicleNo := range vehicles {
		wg.Add(1)
		go func(vehicleNo string) {
			defer wg.Done()

			// 현재 및 이전 API2 데이터 조회
			current, err1 := redisService.GetBusRealtimeByVehicle(routeID, vehicleNo)
			previous, err2 := redisService.GetBusRealtimePreviousByVehicle(routeID, vehicleNo)

			if err1 != nil || current == nil {
				return // 현재 데이터가 없으면 스킵
			}

			currentNodeOrd := es.getNodeOrdFromData(current)

			if err2 != nil || previous == nil {
				// 이전 데이터가 없으면 새로운 버스
				utils.LogInfo("🆕 노선 %s: 새로운 버스 %s (정류장 %d)", routeID, vehicleNo, currentNodeOrd)
				checkChan <- vehicleCheck{vehicleNo: vehicleNo, changed: true}
			} else {
				// 정류장 변경 확인
				previousNodeOrd := es.getNodeOrdFromData(previous)
				if currentNodeOrd != previousNodeOrd {
					utils.LogInfo("📍 노선 %s: 버스 %s 이동 (%d → %d)", routeID, vehicleNo, previousNodeOrd, currentNodeOrd)
					checkChan <- vehicleCheck{vehicleNo: vehicleNo, changed: true}
				}
			}
		}(vehicleNo)
	}

	// 결과 수집용 고루틴
	go func() {
		wg.Wait()
		close(checkChan)
	}()

	// 변경된 차량 목록 수집
	var changed []string
	for check := range checkChan {
		if check.changed {
			changed = append(changed, check.vehicleNo)
		}
	}

	return changed
}

// === 통합 데이터 생성 최적화 - 병렬 처리 ===
func (es *ElasticsearchService) createUnifiedData(redisService *RedisService, routeID string, changedVehicles []string) []models.UnifiedBusLocation {
	if len(changedVehicles) == 0 {
		return []models.UnifiedBusLocation{}
	}

	utils.LogInfo("📋 노선 %s: %d개 변경된 차량의 통합 데이터 생성 시작 (병렬)", routeID, len(changedVehicles))

	type vehicleData struct {
		data *models.UnifiedBusLocation
		err  error
	}

	dataChan := make(chan vehicleData, len(changedVehicles))
	var wg sync.WaitGroup

	// 각 차량을 병렬로 처리
	for _, vehicleNo := range changedVehicles {
		wg.Add(1)
		go func(vehicleNo string) {
			defer wg.Done()

			// 차량별 API1, API2 데이터 조회
			api1Data, err1 := redisService.GetBusLocationByVehicle(routeID, vehicleNo)
			api2Data, err2 := redisService.GetBusRealtimeByVehicle(routeID, vehicleNo)

			if err2 != nil || api2Data == nil {
				utils.LogWarn("차량 %s: API2 데이터 없음", vehicleNo)
				return
			}

			if err1 != nil {
				utils.LogWarn("차량 %s API1 조회 실패: %v", vehicleNo, err1)
			}

			unified := es.mergeVehicleData(routeID, vehicleNo, api1Data, api2Data)
			if unified != nil {
				dataChan <- vehicleData{data: unified, err: nil}
			}
		}(vehicleNo)
	}

	// 결과 수집용 고루틴
	go func() {
		wg.Wait()
		close(dataChan)
	}()

	// 결과 수집
	var unifiedData []models.UnifiedBusLocation
	for vData := range dataChan {
		if vData.err != nil {
			continue
		}
		if vData.data != nil {
			unifiedData = append(unifiedData, *vData.data)
		}
	}

	utils.LogInfo("📋 노선 %s: 통합 데이터 생성 완료 (총 %d개)", routeID, len(unifiedData))
	return unifiedData
}

// Redis 캐시에서 정류장명 조회
func (es *ElasticsearchService) getNodeNameFromCache(routeID string, nodeOrd int) string {
	if es.redisService == nil {
		return ""
	}

	// Redis에서 특정 정류장 정보 조회
	busStop, err := es.redisService.GetBusStop(routeID, nodeOrd)
	if err != nil {
		utils.LogDebug("정류장 캐시 조회 실패 - 노선: %s, nodeOrd: %d, 에러: %v", routeID, nodeOrd, err)
		return ""
	}

	if busStop != nil && busStop.NodeName != "" {
		utils.LogDebug("정류장 캐시 조회 성공 - 노선: %s, nodeOrd: %d, 정류장명: %s", routeID, nodeOrd, busStop.NodeName)
		return busStop.NodeName
	}

	return ""
}

// === 차량별 데이터 병합 === - int 기준 통일
func (es *ElasticsearchService) mergeVehicleData(routeID, vehicleNo string, api1Data, api2Data map[string]interface{}) *models.UnifiedBusLocation {
	if api2Data == nil {
		return nil
	}

	// 통합 데이터 생성
	unified := &models.UnifiedBusLocation{
		RouteID:    routeID,
		DataSource: "unified",
		Timestamp:  time.Now().Unix(),
		VehicleNo:  vehicleNo,
		VehicleID:  vehicleNo,
	}

	// API2 데이터 적용
	if gpsLati, ok := api2Data["gpslati"].(float64); ok {
		unified.GPSLati = gpsLati
	}
	if gpsLong, ok := api2Data["gpslong"].(float64); ok {
		unified.GPSLong = gpsLong
	}
	if nodeOrd, ok := api2Data["nodeord"].(float64); ok {
		unified.NodeOrd = int(nodeOrd)
	}

	// NodeName 처리 - Redis 캐시 활용
	if nodeName, ok := api2Data["nodenm"].(string); ok && nodeName != "" {
		unified.NodeName = &nodeName
	} else {
		// API2에서 nodeName이 없거나 비어있는 경우 Redis 캐시에서 조회
		if unified.NodeOrd > 0 {
			cachedNodeName := es.getNodeNameFromCache(routeID, unified.NodeOrd)
			if cachedNodeName != "" {
				unified.NodeName = &cachedNodeName
				utils.LogDebug("차량 %s: Redis 캐시에서 정류장명 조회 성공 -> %s (nodeOrd: %d)", vehicleNo, cachedNodeName, unified.NodeOrd)
			} else {
				utils.LogWarn("차량 %s: Redis 캐시에서 정류장명 조회 실패 (nodeOrd: %d)", vehicleNo, unified.NodeOrd)
			}
		}
	}

	// RouteName 처리 - int 기준 통일
	if routeNameInt, ok := api2Data["routenm"].(float64); ok {
		// Redis에서 int로 저장된 값이 float64로 언마샬링됨
		routeNameStr := fmt.Sprintf("%.0f", routeNameInt)
		unified.RouteName = &routeNameStr
		utils.LogDebug("차량 %s: routenm(int->string) -> %s", vehicleNo, routeNameStr)
	} else if routeNameInt, ok := api2Data["routenm"].(int); ok {
		// 직접 int로 저장된 경우
		routeNameStr := fmt.Sprintf("%d", routeNameInt)
		unified.RouteName = &routeNameStr
		utils.LogDebug("차량 %s: routenm(int) -> %s", vehicleNo, routeNameStr)
	} else {
		// routenm이 없거나 파싱 실패한 경우 routeID 사용
		unified.RouteName = &routeID
		utils.LogWarn("차량 %s: routenm 파싱 실패, routeID 사용 -> %s", vehicleNo, routeID)
	}

	// RouteType 처리
	if routeType, ok := api2Data["routetp"].(string); ok && routeType != "" {
		unified.RouteType = &routeType
	}

	// API1 데이터 적용 (있는 경우)
	if api1Data != nil {
		utils.LogDebug("차량 %s: API1 데이터 병합 중", vehicleNo)

		if stationSeq, ok := api1Data["stationSeq"].(float64); ok {
			unified.StationSeq = int(stationSeq)
			utils.LogDebug("  - stationSeq: %d", unified.StationSeq)
		}
		if crowded, ok := api1Data["crowded"].(float64); ok {
			val := int(crowded)
			unified.Crowded = &val
			utils.LogDebug("  - crowded: %d", val)
		}
		if remainSeatCnt, ok := api1Data["remainSeatCnt"].(float64); ok {
			val := int(remainSeatCnt)
			unified.RemainSeatCnt = &val
			utils.LogDebug("  - remainSeatCnt: %d", val)
		}
		if stateCd, ok := api1Data["stateCd"].(float64); ok {
			val := int(stateCd)
			unified.StateCd = &val
			utils.LogDebug("  - stateCd: %d", val)
		}
		if vehId, ok := api1Data["vehId"].(float64); ok {
			unified.VehicleID = strconv.FormatInt(int64(vehId), 10)
			utils.LogDebug("  - vehId: %s", unified.VehicleID)
		}

		utils.LogInfo("✅ 차량 %s: API1 데이터 병합 완료", vehicleNo)
	} else {
		utils.LogWarn("⚠️ 차량 %s: API1 데이터 없음", vehicleNo)
	}

	// 운행 상태 판단
	unified.OperationStatus = es.getOperationStatusFromNodeOrd(routeID, unified.NodeOrd)

	return unified
}

// === Elasticsearch 전송 (이벤트 기반) ===
func (es *ElasticsearchService) sendToElasticsearch(unifiedData []models.UnifiedBusLocation) error {
	if len(unifiedData) == 0 {
		return nil
	}

	sendTimer := utils.StartTimer(fmt.Sprintf("ES 전송 %d개 문서", len(unifiedData)))
	defer sendTimer.Stop()

	utils.LogInfo("📤 ES 전송: %d개 문서", len(unifiedData))

	bulkRequest := es.client.Bulk()
	indexName := config.AppConfig.ESUnifiedIndex

	for i, data := range unifiedData {
		docID := data.GetDocumentID()
		esDoc := data.ToMap()

		// 전송 로그
		utils.LogInfo("[%d] 차량: %s, 정류장: %d(%s), 혼잡도: %s, 상태: %s, 노선명: %s",
			i+1, data.VehicleNo, data.NodeOrd,
			es.getNodeNameText(data.NodeName),
			es.getCrowdedText(data.Crowded),
			data.OperationStatus,
			es.getRouteNameText(data.RouteName))

		req := elastic.NewBulkIndexRequest().Index(indexName).Id(docID).Doc(esDoc)
		bulkRequest = bulkRequest.Add(req)
	}

	// 벌크 실행 (타임아웃 설정)
	ctx, cancel := context.WithTimeout(es.ctx, 30*time.Second)
	defer cancel()

	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		return fmt.Errorf("벌크 인덱싱 실패: %v", err)
	}

	if bulkResponse.Errors {
		// 실패한 문서 상세 로깅
		for _, item := range bulkResponse.Items {
			for action, result := range item {
				if result.Error != nil {
					utils.LogError("ES 인덱싱 실패 - Action: %s, ID: %s, Error: %v",
						action, result.Id, result.Error)
				}
			}
		}
		return fmt.Errorf("일부 문서 인덱싱 실패")
	}

	utils.LogInfo("✅ ES 전송 완료: %d개 문서", len(unifiedData))
	return nil
}

// === 헬퍼 함수들 ===

// 데이터에서 nodeOrd 추출
func (es *ElasticsearchService) getNodeOrdFromData(data map[string]interface{}) int {
	if nodeOrd, ok := data["nodeord"].(float64); ok {
		return int(nodeOrd)
	}
	return 0
}

// nodeOrd 기준 운행 상태 판단
func (es *ElasticsearchService) getOperationStatusFromNodeOrd(routeID string, nodeOrd int) string {
	if nodeOrd == 0 {
		return "unknown"
	}

	totalStations := es.getTotalStations(routeID)
	if totalStations == 0 || nodeOrd >= totalStations {
		return "ending"
	}
	return "operating"
}

// 총 정류장 수 조회
func (es *ElasticsearchService) getTotalStations(routeID string) int {
	if es.redisService == nil {
		return 0
	}

	busStops, err := es.redisService.GetAllBusStops(routeID)
	if err != nil {
		return 0
	}

	maxNodeOrd := 0
	for _, stop := range busStops {
		if stop.NodeOrd > maxNodeOrd {
			maxNodeOrd = stop.NodeOrd
		}
	}
	return maxNodeOrd
}

// 혼잡도 텍스트
func (es *ElasticsearchService) getCrowdedText(crowded *int) string {
	if crowded == nil {
		return "정보없음"
	}
	switch *crowded {
	case 1:
		return "여유"
	case 2:
		return "보통"
	case 3:
		return "혼잡"
	case 4:
		return "매우혼잡"
	default:
		return "알수없음"
	}
}

// 정류장명 텍스트
func (es *ElasticsearchService) getNodeNameText(nodeName *string) string {
	if nodeName == nil {
		return "미지정"
	}
	return *nodeName
}

// 노선명 텍스트
func (es *ElasticsearchService) getRouteNameText(routeName *string) string {
	if routeName == nil {
		return "미지정"
	}
	return *routeName
}
