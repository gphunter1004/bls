package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"bus-location-system/config"
	"bus-location-system/models"
	"bus-location-system/services"
	"bus-location-system/utils"
)

func main() {
	// 설정 초기화
	if err := config.Init(); err != nil {
		log.Fatal("설정 초기화 실패:", err)
	}

	utils.LogInfo("버스 위치 정보 데이터 수집 시스템을 시작합니다...")

	// 서비스 초기화 - 병렬로 수행
	timer := utils.StartTimer("서비스 초기화")

	var wg sync.WaitGroup
	var apiService *services.APIService
	var redisService *services.RedisService
	var esService *services.ElasticsearchService
	var initErrors []error

	// API 서비스 초기화 (즉시)
	apiService = services.NewAPIService()
	utils.LogInfo("API 서비스 초기화 완료")

	// Redis와 ES 초기화를 병렬로 수행
	wg.Add(2)

	// Redis 초기화 고루틴
	go func() {
		defer wg.Done()
		var err error
		redisService, err = services.NewRedisService()
		if err != nil {
			initErrors = append(initErrors, fmt.Errorf("Redis 서비스 초기화 실패: %v", err))
		}
	}()

	// ES 초기화 고루틴
	go func() {
		defer wg.Done()
		var err error
		esService, err = services.NewElasticsearchService()
		if err != nil {
			initErrors = append(initErrors, fmt.Errorf("Elasticsearch 서비스 초기화 실패: %v", err))
		}
	}()

	wg.Wait()

	// 초기화 에러 체크
	if len(initErrors) > 0 {
		for _, err := range initErrors {
			log.Printf("초기화 에러: %v", err)
		}
		log.Fatal("서비스 초기화 중 오류 발생")
	}

	defer redisService.Close()

	// ElasticsearchService에 RedisService 참조 설정
	esService.SetRedisService(redisService)
	timer.Stop()

	// 초기 정류장 데이터 수집 및 저장 - 병렬 처리
	if err := collectInitialDataConcurrent(apiService, redisService); err != nil {
		log.Fatal("초기 데이터 수집 실패:", err)
	}

	// 실시간 데이터 수집 시작
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// API 1과 API 2를 독립적으로 실행
	go startBusLocationCollection(ctx, apiService, redisService)
	go startBusRealtimeCollection(ctx, apiService, redisService, esService)

	utils.LogInfo("실시간 데이터 수집이 시작되었습니다.")
	utils.LogInfo("- 버스 위치 정보: %d초 간격", config.AppConfig.BusLocationInterval)
	utils.LogInfo("- 버스 실시간 정보 + ES 동기화: %d초 간격", config.AppConfig.BusRealtimeInterval)

	// 초기 API1 데이터 수집 (비동기)
	go func() {
		utils.LogInfo("🔄 초기 API1 데이터 수집 시작...")
		collectBusLocationsConcurrent(apiService, redisService)
	}()

	// 프로그램 종료 시그널 대기
	waitForShutdown(cancel)
	utils.LogInfo("버스 위치 정보 데이터 수집 시스템을 종료합니다.")
}

// 운영 시간 체크 및 대기
func checkOperatingHours() {
	if !config.AppConfig.OperatingHours.IsEnabled {
		return // 운영 시간 제한이 비활성화된 경우 바로 리턴
	}

	if !config.AppConfig.OperatingHours.IsOperatingTime() {
		waitTime := config.AppConfig.OperatingHours.TimeUntilNextOperating()
		utils.LogInfo("⏰ 현재 운영 시간이 아닙니다. %v 후 운영을 시작합니다.", waitTime)

		// 운영 시간까지 대기 (1분마다 체크)
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for !config.AppConfig.OperatingHours.IsOperatingTime() {
			select {
			case <-ticker.C:
				remaining := config.AppConfig.OperatingHours.TimeUntilNextOperating()
				if remaining > 0 {
					utils.LogInfo("⏰ 운영 시작까지 %v 남았습니다.", remaining.Round(time.Minute))
				}
			}
		}

		utils.LogInfo("🌅 운영 시간이 시작되었습니다!")
	}
}

// 초기 정류장 데이터 수집 - 병렬 처리 개선
func collectInitialDataConcurrent(apiService *services.APIService, redisService *services.RedisService) error {
	timer := utils.StartTimer("초기 데이터 수집")
	defer timer.Stop()

	utils.LogInfo("설정된 노선들의 정류장 데이터를 병렬로 수집합니다...")

	type routeResult struct {
		routeID string
		stops   []models.BusStop
		err     error
	}

	routeCount := len(config.AppConfig.RouteIDs)
	resultChan := make(chan routeResult, routeCount)

	// 워커 풀로 API 호출 병렬화 (최대 5개 동시 호출)
	semaphore := make(chan struct{}, 5)
	var wg sync.WaitGroup

	for _, routeID := range config.AppConfig.RouteIDs {
		wg.Add(1)
		go func(routeID string) {
			defer wg.Done()
			semaphore <- struct{}{}        // 토큰 획득
			defer func() { <-semaphore }() // 토큰 반환

			ggbRouteID := config.AppConfig.GetGGBRouteID(routeID)
			utils.LogInfo("노선 %s (%s)의 정류장 데이터를 수집합니다...", routeID, ggbRouteID)

			busStops, err := apiService.FetchBusStops(ggbRouteID)
			resultChan <- routeResult{routeID: routeID, stops: busStops, err: err}
		}(routeID)
	}

	// 결과 수집용 고루틴
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 결과 처리 및 Redis 저장
	totalStops := 0
	successCount := 0

	for result := range resultChan {
		if result.err != nil {
			utils.LogError("노선 %s 정류장 데이터 수집 실패: %v", result.routeID, result.err)
			continue
		}

		// Redis에 저장 (병렬로 수행하지 않음 - Redis 연결 풀 고려)
		if err := redisService.StoreBusStops(result.routeID, result.stops); err != nil {
			utils.LogError("노선 %s Redis 저장 실패: %v", result.routeID, err)
			continue
		}

		totalStops += len(result.stops)
		successCount++
		utils.LogInfo("노선 %s: %d개 정류장 저장 완료", result.routeID, len(result.stops))
	}

	utils.LogInfo("총 %d개 노선, %d개 정류장 데이터 저장 완료", successCount, totalStops)

	// 정류장 캐시 상태 확인 (병렬)
	var cacheWg sync.WaitGroup
	for _, routeID := range config.AppConfig.RouteIDs {
		cacheWg.Add(1)
		go func(routeID string) {
			defer cacheWg.Done()
			if err := redisService.CheckBusStopCache(routeID); err != nil {
				utils.LogWarn("노선 %s 캐시 상태 확인 실패: %v", routeID, err)
			}
		}(routeID)
	}
	cacheWg.Wait()

	return nil
}

// API 1: 버스 위치 정보 수집 (고루틴) - 컨텍스트 사용
func startBusLocationCollection(ctx context.Context, apiService *services.APIService, redisService *services.RedisService) {
	ticker := time.NewTicker(time.Duration(config.AppConfig.BusLocationInterval) * time.Second)
	defer ticker.Stop()

	utils.LogInfo("버스 위치 정보 수집을 시작합니다 (간격: %d초)", config.AppConfig.BusLocationInterval)

	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("버스 위치 정보 수집을 종료합니다")
			return
		case <-ticker.C:
			// 운영 시간 체크
			if !config.AppConfig.OperatingHours.IsOperatingTime() {
				utils.LogInfo("⏰ 운영 시간 외입니다. API1 수집을 건너뜁니다.")
				continue
			}
			collectBusLocationsConcurrent(apiService, redisService)
		}
	}
}

// 버스 위치 정보 수집 실행 - 병렬 처리 개선
func collectBusLocationsConcurrent(apiService *services.APIService, redisService *services.RedisService) {
	overallTimer := utils.StartTimer("버스 위치 정보 수집")
	defer overallTimer.Stop()

	utils.LogInfo("🚌 API1 버스 위치 정보 병렬 수집 시작")

	// API 수집 단계
	apiTimer := utils.StartTimer("API1 데이터 수집")

	type locationResult struct {
		routeID   string
		locations []models.BusLocationInfo
		err       error
		duration  time.Duration
	}

	routeCount := len(config.AppConfig.RouteIDs)
	resultChan := make(chan locationResult, routeCount)

	// 모든 노선을 병렬로 처리
	var wg sync.WaitGroup
	for _, routeID := range config.AppConfig.RouteIDs {
		wg.Add(1)
		go func(routeID string) {
			defer wg.Done()

			routeTimer := utils.StartTimer(fmt.Sprintf("노선 %s API1 호출", routeID))
			busLocations, err := apiService.FetchBusLocation(routeID)
			duration := routeTimer.Stop()

			resultChan <- locationResult{
				routeID:   routeID,
				locations: busLocations,
				err:       err,
				duration:  duration,
			}
		}(routeID)
	}

	// 결과 수집용 고루틴
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Redis 저장 단계
	redisTimer := utils.StartTimer("Redis 저장")

	totalBuses := 0
	successRoutes := 0
	totalAPITime := time.Duration(0)

	for result := range resultChan {
		totalAPITime += result.duration

		if result.err != nil {
			utils.LogError("노선 %s 버스 위치 정보 수집 실패 (소요시간: %v): %v", result.routeID, result.duration, result.err)
			continue
		}

		if len(result.locations) == 0 {
			utils.LogWarn("노선 %s: 현재 운행 중인 버스가 없습니다 (소요시간: %v)", result.routeID, result.duration)
			continue
		}

		utils.LogInfo("노선 %s: API1에서 %d대 버스 데이터 수집됨 (소요시간: %v)", result.routeID, len(result.locations), result.duration)

		// Redis에 저장
		storeTimer := utils.StartTimer(fmt.Sprintf("노선 %s Redis 저장", result.routeID))
		if err := redisService.StoreBusLocations(result.routeID, result.locations); err != nil {
			storeTimer.Stop()
			utils.LogError("노선 %s 버스 위치 Redis 저장 실패: %v", result.routeID, err)
			continue
		}
		storeDuration := storeTimer.Stop()

		totalBuses += len(result.locations)
		successRoutes++
		utils.LogInfo("✅ 노선 %s: %d대 버스 위치 정보 저장 완료 (API: %v, Redis: %v)",
			result.routeID, len(result.locations), result.duration, storeDuration)
	}

	apiDuration := apiTimer.Stop()
	_ = apiDuration // 사용하지 않는 변수 처리
	redisDuration := redisTimer.Stop()

	if totalBuses > 0 {
		utils.LogInfo("🎯 API1 버스 위치 정보 완료: %d개 노선, %d대 버스 (API평균: %v, Redis총합: %v)",
			successRoutes, totalBuses, totalAPITime/time.Duration(len(config.AppConfig.RouteIDs)), redisDuration)
	} else {
		utils.LogWarn("⚠️ API1 버스 위치 정보: 수집된 데이터가 없습니다 (API총합: %v)", totalAPITime)
	}
}

// API 2: 버스 실시간 위치 정보 수집 (고루틴) - ES 동기화 포함, 병렬 처리
func startBusRealtimeCollection(ctx context.Context, apiService *services.APIService, redisService *services.RedisService, esService *services.ElasticsearchService) {
	ticker := time.NewTicker(time.Duration(config.AppConfig.BusRealtimeInterval) * time.Second)
	defer ticker.Stop()

	utils.LogInfo("버스 실시간 위치 정보 수집 + ES 동기화를 시작합니다 (간격: %d초)", config.AppConfig.BusRealtimeInterval)

	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("버스 실시간 위치 정보 수집을 종료합니다")
			return
		case <-ticker.C:
			// 운영 시간 체크
			if !config.AppConfig.OperatingHours.IsOperatingTime() {
				utils.LogInfo("⏰ 운영 시간 외입니다. API2 수집을 건너뜁니다.")
				continue
			}
			collectBusRealtimeAndSyncConcurrent(apiService, redisService, esService)
		}
	}
}

// 버스 실시간 위치 정보 수집 + ES 동기화 실행 - 병렬 처리
func collectBusRealtimeAndSyncConcurrent(apiService *services.APIService, redisService *services.RedisService, esService *services.ElasticsearchService) {
	overallTimer := utils.StartTimer("버스 실시간 위치 정보 수집 + ES 동기화")
	defer overallTimer.Stop()

	// API 수집 단계
	apiTimer := utils.StartTimer("API2 데이터 수집")

	type realtimeResult struct {
		routeID  string
		realtime []models.BusRealtimeInfo
		err      error
		duration time.Duration
	}

	routeCount := len(config.AppConfig.RouteIDs)
	resultChan := make(chan realtimeResult, routeCount)

	// 모든 노선을 병렬로 처리
	var wg sync.WaitGroup
	for _, routeID := range config.AppConfig.RouteIDs {
		wg.Add(1)
		go func(routeID string) {
			defer wg.Done()

			routeTimer := utils.StartTimer(fmt.Sprintf("노선 %s API2 호출", routeID))
			busRealtime, err := apiService.FetchBusRealtime(routeID)
			duration := routeTimer.Stop()

			resultChan <- realtimeResult{
				routeID:  routeID,
				realtime: busRealtime,
				err:      err,
				duration: duration,
			}
		}(routeID)
	}

	// 결과 수집용 고루틴
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Redis 저장 단계
	redisTimer := utils.StartTimer("Redis 저장")

	totalBuses := 0
	successRoutes := 0
	totalAPITime := time.Duration(0)

	for result := range resultChan {
		totalAPITime += result.duration

		if result.err != nil {
			utils.LogError("노선 %s 버스 실시간 정보 수집 실패 (소요시간: %v): %v", result.routeID, result.duration, result.err)
			continue
		}

		if len(result.realtime) == 0 {
			utils.LogDebug("노선 %s: 현재 실시간 위치 정보가 없습니다 (소요시간: %v)", result.routeID, result.duration)
			continue
		}

		// Redis에 저장
		storeTimer := utils.StartTimer(fmt.Sprintf("노선 %s Redis 저장", result.routeID))
		if err := redisService.StoreBusRealtime(result.routeID, result.realtime); err != nil {
			storeTimer.Stop()
			utils.LogError("노선 %s 버스 실시간 Redis 저장 실패: %v", result.routeID, err)
			continue
		}
		storeDuration := storeTimer.Stop()

		totalBuses += len(result.realtime)
		successRoutes++
		utils.LogDebug("노선 %s: %d대 버스 실시간 정보 저장 완료 (API: %v, Redis: %v)",
			result.routeID, len(result.realtime), result.duration, storeDuration)
	}

	apiDuration := apiTimer.Stop()
	_ = apiDuration // 사용하지 않는 변수 처리
	redisDuration := redisTimer.Stop()

	if totalBuses > 0 {
		utils.LogInfo("버스 실시간 정보 수집 완료: %d개 노선, %d대 버스 (API평균: %v, Redis총합: %v)",
			successRoutes, totalBuses, totalAPITime/time.Duration(len(config.AppConfig.RouteIDs)), redisDuration)

		// ES 동기화를 비동기로 수행
		go func() {
			syncTimer := utils.StartTimer("ES 동기화")
			if err := esService.SyncUnifiedDataFromRedis(redisService); err != nil {
				utils.LogError("Elasticsearch 데이터 동기화 실패: %v", err)
			}
			syncDuration := syncTimer.Stop()
			utils.LogInfo("ES 동기화 완료 (소요시간: %v)", syncDuration)
		}()
	}
}

// Elasticsearch 데이터 동기화 실행 - 비동기
func syncElasticsearchData(esService *services.ElasticsearchService, redisService *services.RedisService) {
	if err := esService.SyncUnifiedDataFromRedis(redisService); err != nil {
		utils.LogError("Elasticsearch 데이터 동기화 실패: %v", err)
	}
}

// 프로그램 종료 시그널 대기 - 컨텍스트 사용
func waitForShutdown(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	utils.LogInfo("시스템이 준비되었습니다. 종료하려면 Ctrl+C를 누르세요.")

	<-c
	utils.LogInfo("종료 신호를 받았습니다. 시스템을 종료합니다...")
	cancel()

	// 잠시 대기하여 고루틴들이 정리될 시간 제공
	time.Sleep(2 * time.Second)
}
