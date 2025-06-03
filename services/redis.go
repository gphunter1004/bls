package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

// 버스 정류장 데이터를 Redis에 저장 (기존)
func (r *RedisService) StoreBusStops(routeID string, busStops []models.BusStop) error {
	pipe := r.client.Pipeline()

	for _, stop := range busStops {
		// JSON으로 직렬화
		stopJSON, err := json.Marshal(stop)
		if err != nil {
			return fmt.Errorf("JSON 직렬화 실패: %v", err)
		}

		// 키 생성: route:{routeID}:stop:{nodeOrd}
		key := fmt.Sprintf("route:%s:stop:%d", routeID, stop.NodeOrd)

		// Redis에 저장 (만료 시간: 24시간)
		pipe.Set(r.ctx, key, stopJSON, 24*time.Hour)

		// 노선별 정류장 순서 목록도 저장
		pipe.ZAdd(r.ctx, fmt.Sprintf("route:%s:stops", routeID), &redis.Z{
			Score:  float64(stop.NodeOrd),
			Member: stop.NodeID,
		})
	}

	// 파이프라인 실행
	_, err := pipe.Exec(r.ctx)
	if err != nil {
		return fmt.Errorf("Redis 저장 실패: %v", err)
	}

	log.Printf("노선 %s의 %d개 정류장 데이터를 Redis에 저장했습니다.", routeID, len(busStops))
	return nil
}

// API 1: 버스 위치 정보를 Redis에 저장
func (r *RedisService) StoreBusLocations(routeID string, busLocations []models.BusLocationInfo) error {
	if len(busLocations) == 0 {
		log.Printf("노선 %s: 저장할 버스 위치 데이터가 없습니다.", routeID)
		return nil
	}

	pipe := r.client.Pipeline()

	// 현재 시간 추가
	timestamp := time.Now().Unix()

	for _, location := range busLocations {
		// 위치 정보에 타임스탬프 추가
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

		// JSON으로 직렬화
		locationJSON, err := json.Marshal(locationData)
		if err != nil {
			return fmt.Errorf("버스 위치 JSON 직렬화 실패: %v", err)
		}

		// 키 생성: bus_location:{routeID}
		key := fmt.Sprintf("gg_bus_location:%s", routeID)

		// Redis에 리스트로 저장 (최신 데이터를 앞에 추가)
		pipe.LPush(r.ctx, key, locationJSON)

		// 리스트 크기 제한 (최신 100개만 유지)
		pipe.LTrim(r.ctx, key, 0, 99)

		// 키 만료 시간 설정 (1시간)
		pipe.Expire(r.ctx, key, 1*time.Hour)
	}

	// 노선별 마지막 업데이트 시간 저장
	lastUpdateKey := fmt.Sprintf("gg_bus_location:%s:last_update", routeID)
	pipe.Set(r.ctx, lastUpdateKey, timestamp, 1*time.Hour)

	// 파이프라인 실행
	_, err := pipe.Exec(r.ctx)
	if err != nil {
		return fmt.Errorf("버스 위치 Redis 저장 실패: %v", err)
	}

	log.Printf("노선 %s의 %d개 버스 위치 데이터를 Redis에 저장했습니다.", routeID, len(busLocations))
	return nil
}

// API 2: 버스 실시간 위치 정보를 Redis에 저장
func (r *RedisService) StoreBusRealtime(routeID string, busRealtime []models.BusRealtimeInfo) error {
	if len(busRealtime) == 0 {
		log.Printf("노선 %s: 저장할 버스 실시간 데이터가 없습니다.", routeID)
		return nil
	}

	pipe := r.client.Pipeline()

	// 현재 시간 추가
	timestamp := time.Now().Unix()

	for _, realtime := range busRealtime {
		// 실시간 정보에 타임스탬프 추가
		realtimeData := map[string]interface{}{
			"gpslati":   realtime.GPSLati,
			"gpslong":   realtime.GPSLong,
			"nodeid":    realtime.NodeID,
			"nodenm":    realtime.NodeName,
			"nodeord":   realtime.NodeOrd,
			"routenm":   realtime.RouteName,
			"routetp":   realtime.RouteType,
			"vehicleno": realtime.VehicleNo,
			"timestamp": timestamp,
		}

		// JSON으로 직렬화
		realtimeJSON, err := json.Marshal(realtimeData)
		if err != nil {
			return fmt.Errorf("버스 실시간 JSON 직렬화 실패: %v", err)
		}

		// 키 생성: bus_realtime:{routeID}
		key := fmt.Sprintf("kr_bus_realtime:%s", routeID)

		// Redis에 리스트로 저장 (최신 데이터를 앞에 추가)
		pipe.LPush(r.ctx, key, realtimeJSON)

		// 리스트 크기 제한 (최신 50개만 유지)
		pipe.LTrim(r.ctx, key, 0, 49)

		// 키 만료 시간 설정 (30분)
		pipe.Expire(r.ctx, key, 30*time.Minute)
	}

	// 노선별 마지막 업데이트 시간 저장
	lastUpdateKey := fmt.Sprintf("kr_bus_realtime:%s:last_update", routeID)
	pipe.Set(r.ctx, lastUpdateKey, timestamp, 30*time.Minute)

	// 파이프라인 실행
	_, err := pipe.Exec(r.ctx)
	if err != nil {
		return fmt.Errorf("버스 실시간 Redis 저장 실패: %v", err)
	}

	log.Printf("노선 %s의 %d개 버스 실시간 데이터를 Redis에 저장했습니다.", routeID, len(busRealtime))
	return nil
}

// 특정 정류장 정보 조회 (기존)
func (r *RedisService) GetBusStop(routeID string, nodeOrd int) (*models.BusStop, error) {
	key := fmt.Sprintf("route:%s:stop:%d", routeID, nodeOrd)

	val, err := r.client.Get(r.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("정류장 정보를 찾을 수 없습니다")
		}
		return nil, fmt.Errorf("Redis 조회 실패: %v", err)
	}

	var busStop models.BusStop
	if err := json.Unmarshal([]byte(val), &busStop); err != nil {
		return nil, fmt.Errorf("JSON 역직렬화 실패: %v", err)
	}

	return &busStop, nil
}
