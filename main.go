package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"bus-location-system/config"
	"bus-location-system/services"
	"bus-location-system/utils"
)

func main() {
	// 설정 초기화
	if err := config.Init(); err != nil {
		log.Fatal("설정 초기화 실패:", err)
	}

	utils.LogInfo("버스 위치 정보 데이터 수집 시스템을 시작합니다...")

	// 서비스 초기화
	timer := utils.StartTimer("서비스 초기화")

	// API 서비스 초기화
	apiService := services.NewAPIService()
	utils.LogInfo("API 서비스 초기화 완료")

	// Redis 서비스 초기화
	redisService, err := services.NewRedisService()
	if err != nil {
		log.Fatal("Redis 서비스 초기화 실패:", err)
	}
	defer redisService.Close()

	// Elasticsearch 서비스 초기화
	esService, err := services.NewElasticsearchService()
	if err != nil {
		log.Fatal("Elasticsearch 서비스 초기화 실패:", err)
	}

	// ElasticsearchService에 RedisService 참조 설정
	esService.SetRedisService(redisService)

	timer.Stop()

	// 초기 정류장 데이터 수집 및 저장
	if err := collectInitialData(apiService, redisService); err != nil {
		log.Fatal("초기 데이터 수집 실패:", err)
	}

	// 실시간 데이터 수집 시작
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// API 1: 버스 위치 정보 수집 (60초 간격)
	wg.Add(1)
	go func() {
		defer wg.Done()
		startBusLocationCollection(apiService, redisService, stopChan)
	}()

	// API 2: 버스 실시간 위치 정보 수집 (10초 간격) - ES 동기화 포함
	wg.Add(1)
	go func() {
		defer wg.Done()
		startBusRealtimeCollection(apiService, redisService, esService, stopChan)
	}()

	utils.LogInfo("실시간 데이터 수집이 시작되었습니다.")
	utils.LogInfo("- 버스 위치 정보: %d초 간격", config.AppConfig.BusLocationInterval)
	utils.LogInfo("- 버스 실시간 정보 + ES 동기화: %d초 간격", config.AppConfig.BusRealtimeInterval)

	// 초기 API1 데이터 수집 (테스트용)
	utils.LogInfo("🔄 초기 API1 데이터 수집 시작...")
	collectBusData("API1", apiService, redisService, nil)

	// 프로그램 종료 시그널 대기
	waitForShutdown(stopChan, &wg)

	utils.LogInfo("버스 위치 정보 데이터 수집 시스템을 종료합니다.")
}

// 초기 정류장 데이터 수집
func collectInitialData(apiService *services.APIService, redisService *services.RedisService) error {
	timer := utils.StartTimer("초기 데이터 수집")
	defer timer.Stop()

	utils.LogInfo("설정된 노선들의 정류장 데이터를 수집합니다...")

	totalStops := 0
	for _, routeID := range config.AppConfig.RouteIDs {
		// GGB 형식으로 변환하여 정류장 데이터 수집
		ggbRouteID := config.AppConfig.GetGGBRouteID(routeID)
		utils.LogInfo("노선 %s (%s)의 정류장 데이터를 수집합니다...", routeID, ggbRouteID)

		busStops, err := apiService.FetchBusStops(ggbRouteID)
		if err != nil {
			utils.LogError("노선 %s 정류장 데이터 수집 실패: %v", routeID, err)
			continue
		}

		// Redis에 저장
		if err := redisService.StoreBusStops(routeID, busStops); err != nil {
			utils.LogError("노선 %s Redis 저장 실패: %v", routeID, err)
			continue
		}

		totalStops += len(busStops)
		utils.LogInfo("노선 %s: %d개 정류장 저장 완료", routeID, len(busStops))

		// API 호출 간격 조절
		time.Sleep(200 * time.Millisecond)
	}

	utils.LogInfo("총 %d개 노선, %d개 정류장 데이터 저장 완료", len(config.AppConfig.RouteIDs), totalStops)

	return nil
}

// API 1: 버스 위치 정보 수집 (고루틴)
func startBusLocationCollection(apiService *services.APIService, redisService *services.RedisService, stopChan chan struct{}) {
	ticker := time.NewTicker(time.Duration(config.AppConfig.BusLocationInterval) * time.Second)
	defer ticker.Stop()

	utils.LogInfo("버스 위치 정보 수집을 시작합니다 (간격: %d초)", config.AppConfig.BusLocationInterval)

	for {
		select {
		case <-stopChan:
			utils.LogInfo("버스 위치 정보 수집을 종료합니다")
			return
		case <-ticker.C:
			collectBusLocations(apiService, redisService)
		}
	}
}

// 버스 위치 정보 수집 실행
func collectBusLocations(apiService *services.APIService, redisService *services.RedisService) {
	timer := utils.StartTimer("버스 위치 정보 수집")
	defer timer.Stop()

	totalBuses := 0
	successRoutes := 0

	utils.LogInfo("🚌 API1 버스 위치 정보 수집 시작")

	for _, routeID := range config.AppConfig.RouteIDs {
		utils.LogInfo("노선 %s API1 데이터 수집 중...", routeID)

		busLocations, err := apiService.FetchBusLocation(routeID)
		if err != nil {
			utils.LogError("노선 %s 버스 위치 정보 수집 실패: %v", routeID, err)
			continue
		}

		if len(busLocations) == 0 {
			utils.LogWarn("노선 %s: 현재 운행 중인 버스가 없습니다 (API1)", routeID)
			continue
		}

		utils.LogInfo("노선 %s: API1에서 %d대 버스 데이터 수집됨", routeID, len(busLocations))

		// 수집된 버스 정보 로깅
		for i, bus := range busLocations {
			utils.LogDebug("  [%d] PlateNo: %s, VehId: %d, StationSeq: %d, Crowded: %d",
				i+1, bus.PlateNo, bus.VehID, bus.StationSeq, bus.Crowded)
		}

		// Redis에 저장
		if err := redisService.StoreBusLocations(routeID, busLocations); err != nil {
			utils.LogError("노선 %s 버스 위치 Redis 저장 실패: %v", routeID, err)
			continue
		}

		totalBuses += len(busLocations)
		successRoutes++
		utils.LogInfo("✅ 노선 %s: %d대 버스 위치 정보 저장 완료", routeID, len(busLocations))

		// API 호출 간격 조절
		time.Sleep(100 * time.Millisecond)
	}

	if totalBuses > 0 {
		utils.LogInfo("🎯 API1 버스 위치 정보: %d개 노선, %d대 버스 데이터 수집 완료", successRoutes, totalBuses)
	} else {
		utils.LogWarn("⚠️ API1 버스 위치 정보: 수집된 데이터가 없습니다")
	}
}

// API 2: 버스 실시간 위치 정보 수집 (고루틴) - ES 동기화 포함
func startBusRealtimeCollection(apiService *services.APIService, redisService *services.RedisService, esService *services.ElasticsearchService, stopChan chan struct{}) {
	ticker := time.NewTicker(time.Duration(config.AppConfig.BusRealtimeInterval) * time.Second)
	defer ticker.Stop()

	utils.LogInfo("버스 실시간 위치 정보 수집 + ES 동기화를 시작합니다 (간격: %d초)", config.AppConfig.BusRealtimeInterval)

	for {
		select {
		case <-stopChan:
			utils.LogInfo("버스 실시간 위치 정보 수집을 종료합니다")
			return
		case <-ticker.C:
			collectBusRealtimeAndSync(apiService, redisService, esService)
		}
	}
}

// 버스 실시간 위치 정보 수집 + ES 동기화 실행
func collectBusRealtimeAndSync(apiService *services.APIService, redisService *services.RedisService, esService *services.ElasticsearchService) {
	timer := utils.StartTimer("버스 실시간 위치 정보 수집 + ES 동기화")
	defer timer.Stop()

	totalBuses := 0
	successRoutes := 0

	for _, routeID := range config.AppConfig.RouteIDs {
		busRealtime, err := apiService.FetchBusRealtime(routeID)
		if err != nil {
			utils.LogError("노선 %s 버스 실시간 정보 수집 실패: %v", routeID, err)
			continue
		}

		if len(busRealtime) == 0 {
			utils.LogDebug("노선 %s: 현재 실시간 위치 정보가 없습니다", routeID)
			continue
		}

		// Redis에 저장
		if err := redisService.StoreBusRealtime(routeID, busRealtime); err != nil {
			utils.LogError("노선 %s 버스 실시간 Redis 저장 실패: %v", routeID, err)
			continue
		}

		totalBuses += len(busRealtime)
		successRoutes++
		utils.LogDebug("노선 %s: %d대 버스 실시간 정보 저장", routeID, len(busRealtime))

		// API 호출 간격 조절
		time.Sleep(100 * time.Millisecond)
	}

	if totalBuses > 0 {
		utils.LogInfo("버스 실시간 정보: %d개 노선, %d대 버스 데이터 수집 완료", successRoutes, totalBuses)

		// API2 데이터 수집 완료 후 즉시 ES 동기화 수행
		syncElasticsearchData(esService, redisService)
	}
}

// Elasticsearch 데이터 동기화 실행
func syncElasticsearchData(esService *services.ElasticsearchService, redisService *services.RedisService) {
	if err := esService.SyncUnifiedDataFromRedis(redisService); err != nil {
		utils.LogError("Elasticsearch 데이터 동기화 실패: %v", err)
	}
}

// 프로그램 종료 시그널 대기
func waitForShutdown(stopChan chan struct{}, wg *sync.WaitGroup) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	utils.LogInfo("시스템이 준비되었습니다. 종료하려면 Ctrl+C를 누르세요.")

	for {
		select {
		case <-c:
			utils.LogInfo("종료 신호를 받았습니다. 시스템을 종료합니다...")
			close(stopChan)
			wg.Wait()
			return
		}
	}
}
