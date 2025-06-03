package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"bus-location-system/config"
	"bus-location-system/models"
)

type APIService struct {
	client *http.Client
}

// APIService 인스턴스 생성
func NewAPIService() *APIService {
	return &APIService{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// 버스 정류장 데이터를 API에서 가져오기 (기존)
func (a *APIService) FetchBusStops(routeID string) ([]models.BusStop, error) {
	// API URL 구성
	url := fmt.Sprintf("%s?serviceKey=%s&cityCode=%s&routeId=%s&_type=%s&numOfRows=%d",
		config.AppConfig.APIBaseURL,
		config.AppConfig.ServiceKey,
		config.AppConfig.CityCode,
		routeID,
		config.AppConfig.ResponseType,
		config.AppConfig.NumOfRows,
	)

	// HTTP 요청 실행
	resp, err := a.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 실패: %v", err)
	}
	defer resp.Body.Close()

	// 응답 상태 코드 확인
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API 응답 오류: HTTP %d", resp.StatusCode)
	}

	// 응답 본문 읽기
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

// API 1: 버스 위치 정보 가져오기
func (a *APIService) FetchBusLocation(routeID string) ([]models.BusLocationInfo, error) {
	// 숫자형 routeID 사용
	numericRouteID := config.AppConfig.GetNumericRouteID(routeID)

	// API URL 구성
	url := fmt.Sprintf("%s?serviceKey=%s&routeId=%s&format=json",
		config.AppConfig.BusLocationAPIURL,
		config.AppConfig.ServiceKey,
		numericRouteID,
	)

	// HTTP 요청 실행
	resp, err := a.client.Get(url)
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

	return response.Response.MsgBody.BusLocationList, nil
}

// API 2: 버스 실시간 위치 정보 가져오기
func (a *APIService) FetchBusRealtime(routeID string) ([]models.BusRealtimeInfo, error) {
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

	// HTTP 요청 실행
	resp, err := a.client.Get(url)
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

	return response.Response.Body.Items.Item, nil
}

// 여러 노선의 정류장 데이터를 가져오기 (기존)
func (a *APIService) FetchMultipleRoutes(routeIDs []string) (map[string][]models.BusStop, error) {
	result := make(map[string][]models.BusStop)

	for _, routeID := range routeIDs {
		busStops, err := a.FetchBusStops(routeID)
		if err != nil {
			return nil, fmt.Errorf("노선 %s 데이터 수집 실패: %v", routeID, err)
		}
		result[routeID] = busStops

		// API 호출 간격 조절 (너무 빈번한 호출 방지)
		time.Sleep(100 * time.Millisecond)
	}

	return result, nil
}

// 모든 노선의 버스 위치 정보 가져오기 (API 1)
func (a *APIService) FetchAllBusLocations(routeIDs []string) (map[string][]models.BusLocationInfo, error) {
	result := make(map[string][]models.BusLocationInfo)

	for _, routeID := range routeIDs {
		busLocations, err := a.FetchBusLocation(routeID)
		if err != nil {
			// 에러 로그만 남기고 계속 진행
			fmt.Printf("노선 %s 버스 위치 데이터 수집 실패: %v\n", routeID, err)
			continue
		}
		result[routeID] = busLocations

		// API 호출 간격 조절
		time.Sleep(100 * time.Millisecond)
	}

	return result, nil
}

// 모든 노선의 버스 실시간 위치 정보 가져오기 (API 2)
func (a *APIService) FetchAllBusRealtime(routeIDs []string) (map[string][]models.BusRealtimeInfo, error) {
	result := make(map[string][]models.BusRealtimeInfo)

	for _, routeID := range routeIDs {
		busRealtime, err := a.FetchBusRealtime(routeID)
		if err != nil {
			// 에러 로그만 남기고 계속 진행
			fmt.Printf("노선 %s 버스 실시간 데이터 수집 실패: %v\n", routeID, err)
			continue
		}
		result[routeID] = busRealtime

		// API 호출 간격 조절
		time.Sleep(100 * time.Millisecond)
	}

	return result, nil
}
