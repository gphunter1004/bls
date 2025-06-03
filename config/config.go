package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// OperatingHours 운영 시간 구조체
type OperatingHours struct {
	StartTime time.Time // 시작 시간 (오늘 날짜 기준)
	EndTime   time.Time // 종료 시간 (다음날일 수 있음)
	IsEnabled bool      // 운영 시간 제한 활성화 여부
}

// Config 구조체
type Config struct {
	// 기본 API 설정 (정류장 정보)
	APIBaseURL   string
	ServiceKey   string
	CityCode     string
	RouteIDs     []string
	ResponseType string
	NumOfRows    int

	// 버스 위치 정보 API (API 1)
	BusLocationAPIURL   string
	BusLocationInterval int

	// 버스 실시간 위치 정보 API (API 2)
	BusRealtimeAPIURL   string
	BusRealtimeInterval int

	// HTTP 클라이언트 설정
	HTTPClientTimeout time.Duration

	// 운영 시간 설정
	OperatingHours OperatingHours

	// Redis 설정
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Elasticsearch 설정
	ESAddr     string
	ESUsername string
	ESPassword string

	// Elasticsearch 통합 인덱스 설정
	ESUnifiedIndex string // 통합 버스 위치 데이터용 인덱스
}

var AppConfig *Config

// 설정 초기화
func Init() error {
	// .env 파일 로드
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: .env 파일을 찾을 수 없습니다. 환경 변수를 직접 사용합니다.")
	}

	AppConfig = &Config{
		APIBaseURL:   getEnvOrDefault("API_BASE_URL", "http://apis.data.go.kr/1613000/BusRouteInfoInqireService/getRouteAcctoThrghSttnList"),
		ServiceKey:   getEnvOrDefault("SERVICE_KEY", ""),
		CityCode:     getEnvOrDefault("CITY_CODE", "31240"),
		RouteIDs:     parseRouteIDs(getEnvOrDefault("ROUTE_IDS", "233000266")),
		ResponseType: getEnvOrDefault("RESPONSE_TYPE", "json"),
		NumOfRows:    getEnvAsIntOrDefault("NUM_OF_ROWS", 100),

		BusLocationAPIURL:   getEnvOrDefault("BUS_LOCATION_API_URL", "https://apis.data.go.kr/6410000/buslocationservice/v2/getBusLocationListv2"),
		BusLocationInterval: getEnvAsIntOrDefault("BUS_LOCATION_INTERVAL", 60),

		BusRealtimeAPIURL:   getEnvOrDefault("BUS_REALTIME_API_URL", "http://apis.data.go.kr/1613000/BusLcInfoInqireService/getRouteAcctoBusLcList"),
		BusRealtimeInterval: getEnvAsIntOrDefault("BUS_REALTIME_INTERVAL", 10),

		// HTTP 클라이언트 타임아웃 설정 (초 단위)
		HTTPClientTimeout: time.Duration(getEnvAsIntOrDefault("HTTP_CLIENT_TIMEOUT", 15)) * time.Second,

		// 운영 시간 설정
		OperatingHours: parseOperatingHours(),

		RedisAddr:     getEnvOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnvOrDefault("REDIS_PASSWORD", ""),
		RedisDB:       getEnvAsIntOrDefault("REDIS_DB", 0),

		ESAddr:     getEnvOrDefault("ELASTICSEARCH_ADDR", "http://localhost:9200"),
		ESUsername: getEnvOrDefault("ELASTICSEARCH_USERNAME", ""),
		ESPassword: getEnvOrDefault("ELASTICSEARCH_PASSWORD", ""),

		// Elasticsearch 통합 인덱스 설정
		ESUnifiedIndex: getEnvOrDefault("ES_UNIFIED_INDEX", "bus-unified-location"),
	}

	// 필수 설정 검증
	if AppConfig.ServiceKey == "" {
		log.Fatal("SERVICE_KEY가 설정되지 않았습니다")
	}

	// HTTP 타임아웃 검증 (최소 5초, 최대 60초)
	if AppConfig.HTTPClientTimeout < 5*time.Second {
		log.Printf("Warning: HTTP_CLIENT_TIMEOUT이 너무 짧습니다 (%v). 최소값 5초로 설정합니다.", AppConfig.HTTPClientTimeout)
		AppConfig.HTTPClientTimeout = 5 * time.Second
	}
	if AppConfig.HTTPClientTimeout > 60*time.Second {
		log.Printf("Warning: HTTP_CLIENT_TIMEOUT이 너무 깁니다 (%v). 최대값 60초로 설정합니다.", AppConfig.HTTPClientTimeout)
		AppConfig.HTTPClientTimeout = 60 * time.Second
	}

	log.Printf("HTTP 클라이언트 타임아웃: %v", AppConfig.HTTPClientTimeout)

	// 운영 시간 로그
	if AppConfig.OperatingHours.IsEnabled {
		log.Printf("운영 시간 설정: %s ~ %s",
			AppConfig.OperatingHours.StartTime.Format("15:04"),
			AppConfig.OperatingHours.EndTime.Format("15:04"))
	} else {
		log.Printf("운영 시간 제한: 비활성화 (24시간 운영)")
	}

	return nil
}

// 운영 시간 파싱
func parseOperatingHours() OperatingHours {
	// 운영 시간 활성화 여부
	enabled := getEnvAsBoolOrDefault("OPERATING_HOURS_ENABLED", false)
	if !enabled {
		return OperatingHours{IsEnabled: false}
	}

	// 시작 시간과 종료 시간 파싱
	startTimeStr := getEnvOrDefault("OPERATING_START_TIME", "05:00")
	endTimeStr := getEnvOrDefault("OPERATING_END_TIME", "23:00")

	startTime, err := parseTimeString(startTimeStr)
	if err != nil {
		log.Printf("Warning: OPERATING_START_TIME 파싱 실패 (%s): %v. 기본값 05:00 사용", startTimeStr, err)
		startTime, _ = parseTimeString("05:00")
	}

	endTime, err := parseTimeString(endTimeStr)
	if err != nil {
		log.Printf("Warning: OPERATING_END_TIME 파싱 실패 (%s): %v. 기본값 23:00 사용", endTimeStr, err)
		endTime, _ = parseTimeString("23:00")
	}

	// 종료 시간이 시작 시간보다 이른 경우 (자정을 넘는 경우)
	if endTime.Before(startTime) {
		endTime = endTime.Add(24 * time.Hour) // 다음날로 설정
	}

	return OperatingHours{
		StartTime: startTime,
		EndTime:   endTime,
		IsEnabled: true,
	}
}

// 시간 문자열 파싱 (HH:MM 형식)
func parseTimeString(timeStr string) (time.Time, error) {
	parts := strings.Split(timeStr, ":")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("시간 형식이 올바르지 않습니다: %s (HH:MM 형식 필요)", timeStr)
	}

	hour, err := strconv.Atoi(parts[0])
	if err != nil || hour < 0 || hour > 23 {
		return time.Time{}, fmt.Errorf("시간(시)이 올바르지 않습니다: %s (0-23)", parts[0])
	}

	minute, err := strconv.Atoi(parts[1])
	if err != nil || minute < 0 || minute > 59 {
		return time.Time{}, fmt.Errorf("시간(분)이 올바르지 않습니다: %s (0-59)", parts[1])
	}

	// 오늘 날짜를 기준으로 시간 생성
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, now.Location()), nil
}

// 현재 시간이 운영 시간 내인지 확인
func (oh *OperatingHours) IsOperatingTime() bool {
	if !oh.IsEnabled {
		return true // 운영 시간 제한이 비활성화된 경우 항상 true
	}

	now := time.Now()

	// 오늘 날짜 기준으로 시작/종료 시간 업데이트
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	startTime := today.Add(time.Duration(oh.StartTime.Hour())*time.Hour + time.Duration(oh.StartTime.Minute())*time.Minute)
	endTime := today.Add(time.Duration(oh.EndTime.Hour())*time.Hour + time.Duration(oh.EndTime.Minute())*time.Minute)

	// 종료 시간이 시작 시간보다 이른 경우 (자정을 넘는 경우)
	if endTime.Before(startTime) {
		endTime = endTime.Add(24 * time.Hour)
		// 현재 시간이 자정을 넘었는지 확인
		if now.Before(startTime) {
			// 자정 이후인 경우, 어제 시작시간과 비교
			startTime = startTime.Add(-24 * time.Hour)
		}
	}

	return now.After(startTime) && now.Before(endTime)
}

// 다음 운영 시작 시간까지의 시간 계산
func (oh *OperatingHours) TimeUntilNextOperating() time.Duration {
	if !oh.IsEnabled {
		return 0 // 운영 시간 제한이 비활성화된 경우
	}

	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	startTime := today.Add(time.Duration(oh.StartTime.Hour())*time.Hour + time.Duration(oh.StartTime.Minute())*time.Minute)

	// 오늘 시작 시간이 이미 지났다면 내일 시작 시간으로 설정
	if now.After(startTime) {
		startTime = startTime.Add(24 * time.Hour)
	}

	return startTime.Sub(now)
}

// RouteID 파싱 (콤마로 구분된 문자열을 배열로 변환)
func parseRouteIDs(routeIDStr string) []string {
	if routeIDStr == "" {
		return []string{}
	}

	routeIDs := strings.Split(routeIDStr, ",")
	for i, routeID := range routeIDs {
		routeIDs[i] = strings.TrimSpace(routeID)
	}

	return routeIDs
}

// 숫자형 RouteID를 GGB 형식으로 변환
func (c *Config) GetGGBRouteID(numericRouteID string) string {
	return "GGB" + numericRouteID
}

// GGB 형식 RouteID를 숫자형으로 변환
func (c *Config) GetNumericRouteID(ggbRouteID string) string {
	if strings.HasPrefix(ggbRouteID, "GGB") {
		return strings.TrimPrefix(ggbRouteID, "GGB")
	}
	return ggbRouteID
}

// 환경 변수 읽기 헬퍼 함수
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsBoolOrDefault(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}
