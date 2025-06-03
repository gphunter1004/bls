package models

import "fmt"

// 기존 API 응답 구조체 (정류장 정보)
type APIResponse struct {
	Response struct {
		Header struct {
			ResultCode string `json:"resultCode"`
			ResultMsg  string `json:"resultMsg"`
		} `json:"header"`
		Body struct {
			Items struct {
				Item []BusStop `json:"item"`
			} `json:"items"`
			NumOfRows  int `json:"numOfRows"`
			PageNo     int `json:"pageNo"`
			TotalCount int `json:"totalCount"`
		} `json:"body"`
	} `json:"response"`
}

// 버스 정류장 구조체
type BusStop struct {
	GPSLati  float64 `json:"gpslati"`
	GPSLong  float64 `json:"gpslong"`
	NodeID   string  `json:"nodeid"`
	NodeName string  `json:"nodenm"`
	NodeNo   int     `json:"nodeno"`
	NodeOrd  int     `json:"nodeord"`
	RouteID  string  `json:"routeid"`
}

// API 1: 버스 위치 정보 응답 구조체
type BusLocationResponse struct {
	Response struct {
		ComMsgHeader string `json:"comMsgHeader"`
		MsgHeader    struct {
			QueryTime     string `json:"queryTime"`
			ResultCode    int    `json:"resultCode"`
			ResultMessage string `json:"resultMessage"`
		} `json:"msgHeader"`
		MsgBody struct {
			BusLocationList []BusLocationInfo `json:"busLocationList"`
		} `json:"msgBody"`
	} `json:"response"`
}

// 버스 위치 정보 구조체 (API 1)
type BusLocationInfo struct {
	Crowded       int    `json:"crowded"`  // 차내혼잡도 (1:여유, 2:보통, 3:혼잡, 4:매우혼잡) 차내혼잡도 제공노선유형 (13:일반형시내버스, 15:따복형시내버스, 23:일반형농어촌버스)
	LowPlate      int    `json:"lowPlate"` // 특수차량여부 (0: 일반버스, 1: 저상버스, 2: 2층버스, 5: 전세버스, 6: 예약버스, 7: 트롤리)
	PlateNo       string `json:"plateNo"`
	RemainSeatCnt int    `json:"remainSeatCnt"` // 차내빈자리수 (-1:정보없음, 0~:빈자리 수) 차내빈자리수 제공노선유형 (11: 직행좌석형시내버스, 12:좌석형시내버스, 14: 광역급행형시내버스, 16: 경기순환버스, 17: 준공영제직행좌석시내버스, 21: 직행좌석형농어촌버스, 22: 좌석형농어촌버스)
	RouteID       int64  `json:"routeId"`       // API1에서 사용하는 숫자형 노선ID
	RouteTypeCd   int    `json:"routeTypeCd"`   // 노선유형코드
	StateCd       int    `json:"stateCd"`       // 상태코드 (0:교차로통과, 1:정류소 도착, 2:정류소 출발)
	StationID     int64  `json:"stationId"`
	StationSeq    int    `json:"stationSeq"` // 정류소순번
	TaglessCd     int    `json:"taglessCd"`  // 태그리스 서비스가 제공되는 차량 여부 (0:일반차량, 1:태그리스차량)
	VehID         int64  `json:"vehId"`
}

// API 2: 버스 실시간 위치 정보 응답 구조체
type BusRealtimeResponse struct {
	Response struct {
		Header struct {
			ResultCode string `json:"resultCode"`
			ResultMsg  string `json:"resultMsg"`
		} `json:"header"`
		Body struct {
			Items struct {
				Item []BusRealtimeInfo `json:"item"`
			} `json:"items"`
			NumOfRows  int `json:"numOfRows"`
			PageNo     int `json:"pageNo"`
			TotalCount int `json:"totalCount"`
		} `json:"body"`
	} `json:"response"`
}

// 버스 실시간 위치 정보 구조체 (API 2)
type BusRealtimeInfo struct {
	GPSLati   float64 `json:"gpslati"`
	GPSLong   float64 `json:"gpslong"`
	NodeID    string  `json:"nodeid"`
	NodeName  string  `json:"nodenm"`
	NodeOrd   int     `json:"nodeord"`
	RouteName int     `json:"routenm"`
	RouteType string  `json:"routetp"`
	VehicleNo string  `json:"vehicleno"`
}

// Redis 키 생성 메서드들

// 정류장 데이터용 Redis 키
func (b *BusStop) GetRedisKey() string {
	return fmt.Sprintf("route:%s:stop:%d", b.RouteID, b.NodeOrd)
}

// 버스 위치 정보용 Redis 키 (API 1)
func (b *BusLocationInfo) GetRedisKey() string {
	return fmt.Sprintf("gg_bus_location:%d", b.RouteID)
}

// 버스 실시간 위치 정보용 Redis 키 (API 2)
func (b *BusRealtimeInfo) GetRedisKey(routeID string) string {
	return fmt.Sprintf("kr_bus_realtime:%s", routeID)
}

// 통합 버스 위치 정보 구조체 (확장용)
type UnifiedBusLocation struct {
	RouteID   string `json:"routeId"`
	Timestamp int64  `json:"timestamp"`
	VehicleID string `json:"vehicleId"`
	VehicleNo string `json:"vehicleNo"`

	// API 1 전용 필드
	StationSeq    int  `json:"stationSeq"`
	Crowded       *int `json:"crowded,omitempty"`
	RemainSeatCnt *int `json:"remainSeatCnt,omitempty"`
	StateCd       *int `json:"stateCd,omitempty"`

	// API 2 전용 필드
	NodeOrd   int     `json:"nodeOrd"`
	GPSLati   float64 `json:"gpsLati"`
	GPSLong   float64 `json:"gpsLong"`
	NodeID    *string `json:"nodeId,omitempty"`
	NodeName  *string `json:"nodeName,omitempty"`
	RouteName *string `json:"routeName,omitempty"`
}
