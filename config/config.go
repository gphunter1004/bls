package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

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

	return nil
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
