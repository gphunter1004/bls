package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"bus-location-system/config"
	"bus-location-system/models"

	"github.com/go-redis/redis/v8"
)

type RedisService struct {
	client *redis.Client
	ctx    context.Context
}

// RedisService 인스턴스 생성
func NewRedisService() (*RedisService, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.AppConfig.RedisAddr,
		Password: config.AppConfig.RedisPassword,
		DB:       config.AppConfig.RedisDB,
	})

	ctx := context.Background()

	// 연결 테스트
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("Redis 연결 실패: %v", err)
	}

	log.Println("Redis 연결 성공")
	return &RedisService{
		client: rdb,
		ctx:    ctx,
	}, nil
}

// 연결 종료
func (r *RedisService) Close() error {
	return r.client.Close()
}

// === 정류장 데이터 저장/조회 ===

// 버스 정류장 데이터를 Redis에 저장
func (r *RedisService) StoreBusStops(routeID string, busStops []models.BusStop) error {
	pipe := r.client.Pipeline()

	for _, stop := range busStops {
		stopJSON, err := json.Marshal(stop)
		if err != nil {
			return fmt.Errorf("JSON 직렬화 실패: %v", err)
		}

		key := fmt.Sprintf("route:%s:stop:%d", routeID, stop.NodeOrd)
		pipe.Set(r.ctx, key, stopJSON, 24*time.Hour)

		pipe.ZAdd(r.ctx, fmt.Sprintf("route:%s:stops", routeID), &redis.Z{
			Score:  float64(stop.NodeOrd),
			Member: stop.NodeID,
		})
	}

	_, err := pipe.Exec(r.ctx)
	if err != nil {
		return fmt.Errorf("Redis 저장 실패: %v", err)
	}

	log.Printf("노선 %s의 %d개 정류장 데이터를 Redis에 저장했습니다.", routeID, len(busStops))
	return nil
}

// 특정 정류장 정보 조회 - 개선된 버전
func (r *RedisService) GetBusStop(routeID string, nodeOrd int) (*models.BusStop, error) {
	key := fmt.Sprintf("route:%s:stop:%d", routeID, nodeOrd)

	val, err := r.client.Get(r.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// 직접 키로 찾지 못한 경우, 모든 정류장을 조회해서 nodeOrd로 찾기
			return r.findBusStopByNodeOrd(routeID, nodeOrd)
		}
		return nil, fmt.Errorf("Redis 조회 실패: %v", err)
	}

	var busStop models.BusStop
	if err := json.Unmarshal([]byte(val), &busStop); err != nil {
		return nil, fmt.Errorf("JSON 역직렬화 실패: %v", err)
	}

	return &busStop, nil
}

// nodeOrd로 정류장 찾기 (보조 메서드)
func (r *RedisService) findBusStopByNodeOrd(routeID string, nodeOrd int) (*models.BusStop, error) {
	// 패턴으로 모든 정류장 키 조회
	pattern := fmt.Sprintf("route:%s:stop:*", routeID)
	keys, err := r.client.Keys(r.ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("정류장 키 조회 실패: %v", err)
	}

	// 각 키에서 데이터를 조회하여 nodeOrd 일치하는 것 찾기
	for _, key := range keys {
		val, err := r.client.Get(r.ctx, key).Result()
		if err != nil {
			continue
		}

		var busStop models.BusStop
		if err := json.Unmarshal([]byte(val), &busStop); err != nil {
			continue
		}

		if busStop.NodeOrd == nodeOrd {
			log.Printf("정류장 발견: 노선 %s, nodeOrd %d -> %s", routeID, nodeOrd, busStop.NodeName)
			return &busStop, nil
		}
	}

	return nil, fmt.Errorf("nodeOrd %d에 해당하는 정류장을 찾을 수 없습니다", nodeOrd)
}

// 노선의 모든 정류장 정보를 nodeOrd 순으로 조회 - 개선된 버전
func (r *RedisService) GetAllBusStops(routeID string) ([]models.BusStop, error) {
	// 패턴으로 모든 정류장 키 조회
	pattern := fmt.Sprintf("route:%s:stop:*", routeID)
	keys, err := r.client.Keys(r.ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("정류장 키 조회 실패: %v", err)
	}

	var busStops []models.BusStop
	for _, key := range keys {
		val, err := r.client.Get(r.ctx, key).Result()
		if err != nil {
			continue
		}

		var busStop models.BusStop
		if err := json.Unmarshal([]byte(val), &busStop); err != nil {
			continue
		}

		busStops = append(busStops, busStop)
	}

	// nodeOrd 순으로 정렬
	sort.Slice(busStops, func(i, j int) bool {
		return busStops[i].NodeOrd < busStops[j].NodeOrd
	})

	log.Printf("노선 %s: 총 %d개 정류장 조회됨", routeID, len(busStops))
	return busStops, nil
}

// 정류장 정보 캐시 상태 확인 (디버깅용)
func (r *RedisService) CheckBusStopCache(routeID string) error {
	pattern := fmt.Sprintf("route:%s:stop:*", routeID)
	keys, err := r.client.Keys(r.ctx, pattern).Result()
	if err != nil {
		return fmt.Errorf("정류장 캐시 확인 실패: %v", err)
	}

	log.Printf("노선 %s 정류장 캐시 상태:", routeID)
	log.Printf("- 총 %d개 정류장 캐시됨", len(keys))

	if len(keys) > 0 {
		// 첫 번째 정류장 정보 샘플 출력
		val, err := r.client.Get(r.ctx, keys[0]).Result()
		if err == nil {
			var busStop models.BusStop
			if err := json.Unmarshal([]byte(val), &busStop); err == nil {
				log.Printf("- 샘플 정류장: nodeOrd=%d, 이름=%s", busStop.NodeOrd, busStop.NodeName)
			}
		}
	}

	return nil
}

// === API 1: 차량별 저장/조회 ===

// API 1: 버스 위치 정보를 차량별로 Redis에 저장
func (r *RedisService) StoreBusLocations(routeID string, busLocations []models.BusLocationInfo) error {
	if len(busLocations) == 0 {
		log.Printf("노선 %s: 저장할 버스 위치 데이터가 없습니다.", routeID)
		return nil
	}

	pipe := r.client.Pipeline()
	timestamp := time.Now().Unix()

	log.Printf("📦 노선 %s: API1 데이터 차량별 저장 시작 (%d개)", routeID, len(busLocations))

	for i, location := range busLocations {
		// 차량별 개별 저장
		locationData := map[string]interface{}{
			"crowded":       location.Crowded,
			"lowPlate":      location.LowPlate,
			"plateNo":       location.PlateNo,
			"remainSeatCnt": location.RemainSeatCnt,
			"routeId":       location.RouteID,
			"routeTypeCd":   location.RouteTypeCd,
			"stateCd":       location.StateCd,
			"stationId":     location.StationID,
			"stationSeq":    location.StationSeq,
			"taglessCd":     location.TaglessCd,
			"vehId":         location.VehID,
			"timestamp":     timestamp,
		}

		locationJSON, err := json.Marshal(locationData)
		if err != nil {
			return fmt.Errorf("버스 위치 JSON 직렬화 실패: %v", err)
		}

		// 차량별 키: route:{routeID}:bus:{plateNo}:api1
		busKey := fmt.Sprintf("route:%s:bus:%s:api1", routeID, location.PlateNo)
		pipe.Set(r.ctx, busKey, locationJSON, 1*time.Hour)

		// 노선의 활성 버스 목록에 추가
		activeBusesKey := fmt.Sprintf("route:%s:active_buses", routeID)
		pipe.SAdd(r.ctx, activeBusesKey, location.PlateNo)
		pipe.Expire(r.ctx, activeBusesKey, 1*time.Hour)

		log.Printf("  [%d] API1 저장: %s → %s", i+1, location.PlateNo, busKey)
	}

	_, err := pipe.Exec(r.ctx)
	if err != nil {
		return fmt.Errorf("버스 위치 Redis 저장 실패: %v", err)
	}

	log.Printf("✅ 노선 %s의 %d개 버스 위치 데이터를 차량별로 Redis에 저장했습니다.", routeID, len(busLocations))
	return nil
}

// === API 2: 차량별 저장/조회 ===

// API 2: 버스 실시간 위치 정보를 차량별로 Redis에 저장 - int 기준 통일
func (r *RedisService) StoreBusRealtime(routeID string, busRealtime []models.BusRealtimeInfo) error {
	if len(busRealtime) == 0 {
		log.Printf("노선 %s: 저장할 버스 실시간 데이터가 없습니다.", routeID)
		return nil
	}

	pipe := r.client.Pipeline()
	timestamp := time.Now().Unix()

	for _, realtime := range busRealtime {
		// 차량별 개별 저장 - routenm을 int 그대로 저장
		realtimeData := map[string]interface{}{
			"gpslati":   realtime.GPSLati,
			"gpslong":   realtime.GPSLong,
			"nodeid":    realtime.NodeID,
			"nodenm":    realtime.NodeName,
			"nodeord":   realtime.NodeOrd,
			"routenm":   realtime.RouteName, // int 값 그대로 저장
			"routetp":   realtime.RouteType,
			"vehicleno": realtime.VehicleNo,
			"timestamp": timestamp,
		}

		realtimeJSON, err := json.Marshal(realtimeData)
		if err != nil {
			return fmt.Errorf("버스 실시간 JSON 직렬화 실패: %v", err)
		}

		// 차량별 키: route:{routeID}:bus:{vehicleNo}:api2
		busKey := fmt.Sprintf("route:%s:bus:%s:api2", routeID, realtime.VehicleNo)

		// 이전 데이터 보관을 위해 현재 데이터를 prev로 이동
		prevKey := fmt.Sprintf("route:%s:bus:%s:api2:prev", routeID, realtime.VehicleNo)
		currentData, err := r.client.Get(r.ctx, busKey).Result()
		if err == nil {
			pipe.Set(r.ctx, prevKey, currentData, 30*time.Minute)
		}

		// 새 데이터 저장
		pipe.Set(r.ctx, busKey, realtimeJSON, 30*time.Minute)

		// 노선의 활성 버스 목록에 추가
		activeBusesKey := fmt.Sprintf("route:%s:active_buses", routeID)
		pipe.SAdd(r.ctx, activeBusesKey, realtime.VehicleNo)
		pipe.Expire(r.ctx, activeBusesKey, 30*time.Minute)

		// 디버그 로그 - int 값 확인
		log.Printf("API2 저장: 차량 %s, routenm: %d", realtime.VehicleNo, realtime.RouteName)
	}

	_, err := pipe.Exec(r.ctx)
	if err != nil {
		return fmt.Errorf("버스 실시간 Redis 저장 실패: %v", err)
	}

	log.Printf("노선 %s의 %d개 버스 실시간 데이터를 차량별로 Redis에 저장했습니다.", routeID, len(busRealtime))
	return nil
}

// === 차량별 조회 함수들 ===

// 특정 차량의 API1 데이터 조회 (차량번호 매칭 포함)
func (r *RedisService) GetBusLocationByVehicle(routeID, vehicleNo string) (map[string]interface{}, error) {
	// 1. 정확한 차량번호로 먼저 시도
	key := fmt.Sprintf("route:%s:bus:%s:api1", routeID, vehicleNo)
	val, err := r.client.Get(r.ctx, key).Result()

	if err == nil {
		var locationData map[string]interface{}
		if err := json.Unmarshal([]byte(val), &locationData); err == nil {
			return locationData, nil
		}
	}

	// 2. 활성 버스 목록에서 부분 매칭 시도
	activeBuses, err := r.GetActiveBuses(routeID)
	if err != nil {
		return nil, fmt.Errorf("활성 버스 목록 조회 실패: %v", err)
	}

	for _, plateNo := range activeBuses {
		// 차량번호 매칭 (부분 일치 포함)
		if r.isVehicleMatch(plateNo, vehicleNo) {
			key = fmt.Sprintf("route:%s:bus:%s:api1", routeID, plateNo)
			val, err = r.client.Get(r.ctx, key).Result()

			if err == nil {
				var locationData map[string]interface{}
				if err := json.Unmarshal([]byte(val), &locationData); err == nil {
					log.Printf("차량번호 매칭 성공: API2(%s) ↔ API1(%s)", vehicleNo, plateNo)
					return locationData, nil
				}
			}
		}
	}

	// 데이터 없음
	return nil, nil
}

// 차량번호 매칭 확인
func (r *RedisService) isVehicleMatch(plateNo, vehicleNo string) bool {
	// 완전 일치
	if plateNo == vehicleNo {
		return true
	}

	// 부분 일치 (API1과 API2의 차량번호 형식이 다를 수 있음)
	// 예: API1="경기70바1234", API2="1234" 또는 그 반대
	if len(plateNo) > 4 && len(vehicleNo) > 4 {
		// 둘 다 긴 경우, 끝 4자리 비교
		plateEnd := plateNo[len(plateNo)-4:]
		vehicleEnd := vehicleNo[len(vehicleNo)-4:]
		if plateEnd == vehicleEnd {
			return true
		}
	}

	// 포함 관계 확인
	if len(plateNo) > len(vehicleNo) {
		return strings.Contains(plateNo, vehicleNo)
	} else if len(vehicleNo) > len(plateNo) {
		return strings.Contains(vehicleNo, plateNo)
	}

	return false
}

// 특정 차량의 API2 현재 데이터 조회
func (r *RedisService) GetBusRealtimeByVehicle(routeID, vehicleNo string) (map[string]interface{}, error) {
	key := fmt.Sprintf("route:%s:bus:%s:api2", routeID, vehicleNo)

	val, err := r.client.Get(r.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // 데이터 없음
		}
		return nil, fmt.Errorf("Redis 조회 실패: %v", err)
	}

	var realtimeData map[string]interface{}
	if err := json.Unmarshal([]byte(val), &realtimeData); err != nil {
		return nil, fmt.Errorf("JSON 역직렬화 실패: %v", err)
	}

	return realtimeData, nil
}

// 특정 차량의 API2 이전 데이터 조회
func (r *RedisService) GetBusRealtimePreviousByVehicle(routeID, vehicleNo string) (map[string]interface{}, error) {
	key := fmt.Sprintf("route:%s:bus:%s:api2:prev", routeID, vehicleNo)

	val, err := r.client.Get(r.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // 데이터 없음
		}
		return nil, fmt.Errorf("Redis 조회 실패: %v", err)
	}

	var realtimeData map[string]interface{}
	if err := json.Unmarshal([]byte(val), &realtimeData); err != nil {
		return nil, fmt.Errorf("JSON 역직렬화 실패: %v", err)
	}

	return realtimeData, nil
}

// 노선의 모든 활성 버스 목록 조회
func (r *RedisService) GetActiveBuses(routeID string) ([]string, error) {
	key := fmt.Sprintf("route:%s:active_buses", routeID)

	vehicles, err := r.client.SMembers(r.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return []string{}, nil
		}
		return nil, fmt.Errorf("활성 버스 목록 조회 실패: %v", err)
	}

	return vehicles, nil
}
