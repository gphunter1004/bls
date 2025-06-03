package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"bus-location-system/config"
	"bus-location-system/models"
)

type APIService struct {
	client     *http.Client
	transport  *http.Transport
	clientPool sync.Pool
}

// APIService 인스턴스 생성 - 커넥션 풀 최적화
func NewAPIService() *APIService {
	// HTTP Transport 최적화
	transport := &http.Transport{
		MaxIdleConns:        100,              // 전체 최대 idle 연결
		MaxIdleConnsPerHost: 20,               // 호스트당 최대 idle 연결
		IdleConnTimeout:     90 * time.Second, // idle 연결 타임아웃
		DisableKeepAlives:   false,            // Keep-Alive 활성화
		DisableCompression:  false,            // 자동 압축 해제 활성화
		WriteBufferSize:     32 * 1024,        // 쓰기 버퍼 크기
		ReadBufferSize:      32 * 1024,        // 읽기 버퍼 크기
	}

	client := &http.Client{
		Timeout:   config.AppConfig.HTTPClientTimeout, // config에서 타임아웃 설정
		Transport: transport,
	}

	return &APIService{
		client:    client,
		transport: transport,
		clientPool: sync.Pool{
			New: func() interface{} {
				return &http.Client{
					Timeout:   config.AppConfig.HTTPClientTimeout, // config에서 타임아웃 설정
					Transport: transport,
				}
			},
		},
	}
}

// HTTP 클라이언트 풀에서 가져오기
func (a *APIService) getClient() *http.Client {
	return a.clientPool.Get().(*http.Client)
}

// HTTP 클라이언트 풀에 반환
func (a *APIService) putClient(client *http.Client) {
	a.clientPool.Put(client)
}

// 버스 정류장 데이터를 API에서 가져오기 - 최적화된 버전
func (a *APIService) FetchBusStops(routeID string) ([]models.BusStop, error) {
	// 클라이언트 풀 사용
	client := a.getClient()
	defer a.putClient(client)

	// API URL 구성
	url := fmt.Sprintf("%s?serviceKey=%s&cityCode=%s&routeId=%s&_type=%s&numOfRows=%d",
		config.AppConfig.APIBaseURL,
		config.AppConfig.ServiceKey,
		config.AppConfig.CityCode,
		routeID,
		config.AppConfig.ResponseType,
		config.AppConfig.NumOfRows,
	)

	// HTTP 요청 생성 및 헤더 최적화
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 생성 실패: %v", err)
	}

	// 헤더 최적화 - gzip 제거
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("User-Agent", "BusLocationSystem/1.0")

	// HTTP 요청 실행
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 실패: %v", err)
	}
	defer resp.Body.Close()

	// 응답 상태 코드 확인
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API 응답 오류: HTTP %d", resp.StatusCode)
	}

	// 응답 본문 읽기 - 버퍼 크기 최적화
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("응답 본문 읽기 실패: %v", err)
	}

	// JSON 파싱
	var apiResponse models.APIResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	// API 응답 코드 확인
	if apiResponse.Response.Header.ResultCode != "00" {
		return nil, fmt.Errorf("API 오류: %s - %s",
			apiResponse.Response.Header.ResultCode,
			apiResponse.Response.Header.ResultMsg)
	}

	return apiResponse.Response.Body.Items.Item, nil
}

// API 1: 버스 위치 정보 가져오기 - 단일/배열 응답 처리
func (a *APIService) FetchBusLocation(routeID string) ([]models.BusLocationInfo, error) {
	client := a.getClient()
	defer a.putClient(client)

	// 숫자형 routeID 사용
	numericRouteID := config.AppConfig.GetNumericRouteID(routeID)

	// API URL 구성
	url := fmt.Sprintf("%s?serviceKey=%s&routeId=%s&format=json",
		config.AppConfig.BusLocationAPIURL,
		config.AppConfig.ServiceKey,
		numericRouteID,
	)

	// HTTP 요청 생성 및 헤더 최적화
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("버스 위치 API 요청 생성 실패: %v", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connection", "keep-alive")

	// HTTP 요청 실행
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("버스 위치 API 요청 실패: %v", err)
	}
	defer resp.Body.Close()

	// 응답 상태 코드 확인
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("버스 위치 API 응답 오류: HTTP %d", resp.StatusCode)
	}

	// 응답 본문 읽기
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("버스 위치 API 응답 읽기 실패: %v", err)
	}

	// JSON 파싱
	var response models.BusLocationResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("버스 위치 API JSON 파싱 실패: %v", err)
	}

	// API 응답 코드 확인
	if response.Response.MsgHeader.ResultCode != 0 {
		return nil, fmt.Errorf("버스 위치 API 오류: %d - %s",
			response.Response.MsgHeader.ResultCode,
			response.Response.MsgHeader.ResultMessage)
	}

	// busLocationList 파싱 (배열 또는 단일 객체 처리)
	return a.parseBusLocationList(response.Response.MsgBody.BusLocationList)
}

// busLocationList 파싱 헬퍼 함수 (배열 또는 단일 객체 처리)
func (a *APIService) parseBusLocationList(busLocationList interface{}) ([]models.BusLocationInfo, error) {
	if busLocationList == nil {
		return []models.BusLocationInfo{}, nil
	}

	// JSON으로 다시 마샬링 후 언마샬링하여 타입 확인
	busLocationJSON, err := json.Marshal(busLocationList)
	if err != nil {
		return nil, fmt.Errorf("busLocationList 마샬링 실패: %v", err)
	}

	// 배열인지 확인
	var busLocationArray []models.BusLocationInfo
	if err := json.Unmarshal(busLocationJSON, &busLocationArray); err == nil {
		// 배열로 파싱 성공
		return busLocationArray, nil
	}

	// 단일 객체인지 확인
	var busLocationSingle models.BusLocationInfo
	if err := json.Unmarshal(busLocationJSON, &busLocationSingle); err == nil {
		// 단일 객체로 파싱 성공
		return []models.BusLocationInfo{busLocationSingle}, nil
	}

	return nil, fmt.Errorf("busLocationList 파싱 실패: 배열도 단일 객체도 아님")
}

// API 2: 버스 실시간 위치 정보 가져오기 - 단일/배열 응답 처리
func (a *APIService) FetchBusRealtime(routeID string) ([]models.BusRealtimeInfo, error) {
	client := a.getClient()
	defer a.putClient(client)

	// GGB 형식 routeID 사용
	ggbRouteID := routeID
	if !strings.HasPrefix(routeID, "GGB") {
		ggbRouteID = config.AppConfig.GetGGBRouteID(routeID)
	}

	// API URL 구성
	url := fmt.Sprintf("%s?serviceKey=%s&cityCode=%s&routeId=%s&_type=%s&numOfRows=%d",
		config.AppConfig.BusRealtimeAPIURL,
		config.AppConfig.ServiceKey,
		config.AppConfig.CityCode,
		ggbRouteID,
		config.AppConfig.ResponseType,
		config.AppConfig.NumOfRows,
	)

	// HTTP 요청 생성 및 헤더 최적화
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("버스 실시간 API 요청 생성 실패: %v", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connection", "keep-alive")

	// HTTP 요청 실행
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("버스 실시간 API 요청 실패: %v", err)
	}
	defer resp.Body.Close()

	// 응답 상태 코드 확인
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("버스 실시간 API 응답 오류: HTTP %d", resp.StatusCode)
	}

	// 응답 본문 읽기
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("버스 실시간 API 응답 읽기 실패: %v", err)
	}

	// JSON 파싱
	var response models.BusRealtimeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("버스 실시간 API JSON 파싱 실패: %v", err)
	}

	// API 응답 코드 확인
	if response.Response.Header.ResultCode != "00" {
		return nil, fmt.Errorf("버스 실시간 API 오류: %s - %s",
			response.Response.Header.ResultCode,
			response.Response.Header.ResultMsg)
	}

	// item 파싱 (배열 또는 단일 객체 처리)
	return a.parseBusRealtimeList(response.Response.Body.Items.Item)
}

// item 파싱 헬퍼 함수 (배열 또는 단일 객체 처리)
func (a *APIService) parseBusRealtimeList(item interface{}) ([]models.BusRealtimeInfo, error) {
	if item == nil {
		return []models.BusRealtimeInfo{}, nil
	}

	// JSON으로 다시 마샬링 후 언마샬링하여 타입 확인
	itemJSON, err := json.Marshal(item)
	if err != nil {
		return nil, fmt.Errorf("item 마샬링 실패: %v", err)
	}

	// 배열인지 확인
	var itemArray []models.BusRealtimeInfo
	if err := json.Unmarshal(itemJSON, &itemArray); err == nil {
		// 배열로 파싱 성공
		return itemArray, nil
	}

	// 단일 객체인지 확인
	var itemSingle models.BusRealtimeInfo
	if err := json.Unmarshal(itemJSON, &itemSingle); err == nil {
		// 단일 객체로 파싱 성공
		return []models.BusRealtimeInfo{itemSingle}, nil
	}

	return nil, fmt.Errorf("item 파싱 실패: 배열도 단일 객체도 아님")
}

// 병렬 배치 API 호출 - 새로운 최적화 메서드
func (a *APIService) FetchBusLocationsBatch(routeIDs []string) (map[string][]models.BusLocationInfo, error) {
	type result struct {
		routeID   string
		locations []models.BusLocationInfo
		err       error
	}

	resultChan := make(chan result, len(routeIDs))
	var wg sync.WaitGroup

	// 워커 풀로 동시 API 호출 제한 (최대 10개)
	semaphore := make(chan struct{}, 10)

	for _, routeID := range routeIDs {
		wg.Add(1)
		go func(routeID string) {
			defer wg.Done()
			semaphore <- struct{}{}        // 토큰 획득
			defer func() { <-semaphore }() // 토큰 반환

			locations, err := a.FetchBusLocation(routeID)
			resultChan <- result{
				routeID:   routeID,
				locations: locations,
				err:       err,
			}
		}(routeID)
	}

	// 결과 수집용 고루틴
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 결과 취합
	results := make(map[string][]models.BusLocationInfo)
	for res := range resultChan {
		if res.err != nil {
			// 에러는 로그만 남기고 계속 진행
			fmt.Printf("노선 %s API1 호출 실패: %v\n", res.routeID, res.err)
			continue
		}
		results[res.routeID] = res.locations
	}

	return results, nil
}

// 병렬 배치 API 호출 - 실시간 데이터용
func (a *APIService) FetchBusRealtimeBatch(routeIDs []string) (map[string][]models.BusRealtimeInfo, error) {
	type result struct {
		routeID  string
		realtime []models.BusRealtimeInfo
		err      error
	}

	resultChan := make(chan result, len(routeIDs))
	var wg sync.WaitGroup

	// 워커 풀로 동시 API 호출 제한 (최대 10개)
	semaphore := make(chan struct{}, 10)

	for _, routeID := range routeIDs {
		wg.Add(1)
		go func(routeID string) {
			defer wg.Done()
			semaphore <- struct{}{}        // 토큰 획득
			defer func() { <-semaphore }() // 토큰 반환

			realtime, err := a.FetchBusRealtime(routeID)
			resultChan <- result{
				routeID:  routeID,
				realtime: realtime,
				err:      err,
			}
		}(routeID)
	}

	// 결과 수집용 고루틴
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 결과 취합
	results := make(map[string][]models.BusRealtimeInfo)
	for res := range resultChan {
		if res.err != nil {
			// 에러는 로그만 남기고 계속 진행
			fmt.Printf("노선 %s API2 호출 실패: %v\n", res.routeID, res.err)
			continue
		}
		results[res.routeID] = res.realtime
	}

	return results, nil
}

// 연결 상태 확인 및 정리
func (a *APIService) Close() {
	if a.transport != nil {
		a.transport.CloseIdleConnections()
	}
}

// 여러 노선의 정류장 데이터를 가져오기 (기존 - 호환성 유지)
func (a *APIService) FetchMultipleRoutes(routeIDs []string) (map[string][]models.BusStop, error) {
	result := make(map[string][]models.BusStop)

	for _, routeID := range routeIDs {
		busStops, err := a.FetchBusStops(routeID)
		if err != nil {
			return nil, fmt.Errorf("노선 %s 데이터 수집 실패: %v", routeID, err)
		}
		result[routeID] = busStops

		// API 호출 간격 조절 제거 (병렬 처리로 대체)
	}

	return result, nil
}

// 모든 노선의 버스 위치 정보 가져오기 (API 1) - 레거시 호환
func (a *APIService) FetchAllBusLocations(routeIDs []string) (map[string][]models.BusLocationInfo, error) {
	return a.FetchBusLocationsBatch(routeIDs)
}

// 모든 노선의 버스 실시간 위치 정보 가져오기 (API 2) - 레거시 호환
func (a *APIService) FetchAllBusRealtime(routeIDs []string) (map[string][]models.BusRealtimeInfo, error) {
	return a.FetchBusRealtimeBatch(routeIDs)
}
