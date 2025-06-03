package utils

import (
	"fmt"
	"log"
	"math"
	"time"
)

// 로그 레벨
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

var currentLogLevel = LogLevelInfo

// 로그 레벨 설정
func SetLogLevel(level LogLevel) {
	currentLogLevel = level
}

// 현재 로그 레벨 조회
func GetLogLevel() LogLevel {
	return currentLogLevel
}

// 디버그 로그
func LogDebug(format string, args ...interface{}) {
	if currentLogLevel <= LogLevelDebug {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// 정보 로그
func LogInfo(format string, args ...interface{}) {
	if currentLogLevel <= LogLevelInfo {
		log.Printf("[INFO] "+format, args...)
	}
}

// 경고 로그
func LogWarn(format string, args ...interface{}) {
	if currentLogLevel <= LogLevelWarn {
		log.Printf("[WARN] "+format, args...)
	}
}

// 에러 로그
func LogError(format string, args ...interface{}) {
	if currentLogLevel <= LogLevelError {
		log.Printf("[ERROR] "+format, args...)
	}
}

// 두 GPS 좌표 간의 거리 계산 (Haversine formula)
func CalculateDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371 // 지구 반지름 (km)

	// 라디안으로 변환
	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	deltaLat := (lat2 - lat1) * math.Pi / 180
	deltaLon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}

// 재시도 함수
func RetryWithBackoff(fn func() error, maxRetries int, baseDelay time.Duration) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			return nil
		}

		if i == maxRetries-1 {
			break
		}

		// 지수 백오프
		delay := time.Duration(math.Pow(2, float64(i))) * baseDelay
		LogWarn("재시도 %d/%d, %v 후 다시 시도: %v", i+1, maxRetries, delay, err)
		time.Sleep(delay)
	}

	return fmt.Errorf("최대 재시도 횟수 초과: %v", err)
}

// 안전한 문자열 변환
func SafeString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

// 안전한 정수 변환
func SafeInt(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		// 문자열에서 숫자 변환 시도
		if len(v) == 0 {
			return 0
		}
		// 간단한 정수 파싱 (더 복잡한 경우는 strconv 사용)
		return 0
	default:
		return 0
	}
}

// 안전한 실수 변환
func SafeFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0.0
	}
}

// 현재 시간을 문자열로 반환
func CurrentTimeString() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

// Unix 타임스탬프를 시간 문자열로 변환
func UnixToTimeString(timestamp int64) string {
	return time.Unix(timestamp, 0).Format("2006-01-02 15:04:05")
}

// 배열에서 중복 제거
func RemoveDuplicateStrings(slice []string) []string {
	keys := make(map[string]bool)
	var result []string

	for _, item := range slice {
		if !keys[item] {
			keys[item] = true
			result = append(result, item)
		}
	}

	return result
}

// 문자열 배열에서 특정 값 찾기
func ContainsString(slice []string, target string) bool {
	for _, item := range slice {
		if item == target {
			return true
		}
	}
	return false
}

// 성능 측정 헬퍼
type Timer struct {
	start time.Time
	name  string
}

// 타이머 시작
func StartTimer(name string) *Timer {
	return &Timer{
		start: time.Now(),
		name:  name,
	}
}

// 타이머 종료 및 소요 시간 로그
func (t *Timer) Stop() time.Duration {
	duration := time.Since(t.start)
	LogInfo("%s 소요 시간: %v", t.name, duration)
	return duration
}

// 메모리 사용량 포맷팅
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// 에러 체크 및 로그
func CheckError(err error, context string) bool {
	if err != nil {
		LogError("%s: %v", context, err)
		return true
	}
	return false
}
