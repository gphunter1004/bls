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

// API 1: 버스 위치 정보 응답 구조체 - 유연한 구조
type BusLocationResponse struct {
	Response struct {
		ComMsgHeader string `json:"comMsgHeader"`
		MsgHeader    struct {
			QueryTime     string `json:"queryTime"`
			ResultCode    int    `json:"resultCode"`
			ResultMessage string `json:"resultMessage"`
		} `json:"msgHeader"`
		MsgBody struct {
			BusLocationList interface{} `json:"busLocationList"` // 배열 또는 단일 객체
		} `json:"msgBody"`
	} `json:"response"`
}

// 버스 위치 정보 구조체 (API 1)
type BusLocationInfo struct {
	Crowded       int    `json:"crowded"`  // 차내혼잡도 (1:여유, 2:보통, 3:혼잡, 4:매우혼잡)
	LowPlate      int    `json:"lowPlate"` // 특수차량여부 (0: 일반버스, 1: 저상버스, 2: 2층버스, 5: 전세버스, 6: 예약버스, 7: 트롤리)
	PlateNo       string `json:"plateNo"`
	RemainSeatCnt int    `json:"remainSeatCnt"` // 차내빈자리수 (-1:정보없음, 0~:빈자리 수)
	RouteID       int64  `json:"routeId"`       // API1에서 사용하는 숫자형 노선ID
	RouteTypeCd   int    `json:"routeTypeCd"`   // 노선유형코드
	StateCd       int    `json:"stateCd"`       // 상태코드 (0:교차로통과, 1:정류소 도착, 2:정류소 출발)
	StationID     int64  `json:"stationId"`
	StationSeq    int    `json:"stationSeq"` // 정류소순번
	TaglessCd     int    `json:"taglessCd"`  // 태그리스 서비스가 제공되는 차량 여부 (0:일반차량, 1:태그리스차량)
	VehID         int64  `json:"vehId"`
}

// API 2: 버스 실시간 위치 정보 응답 구조체 - 유연한 구조
type BusRealtimeResponse struct {
	Response struct {
		Header struct {
			ResultCode string `json:"resultCode"`
			ResultMsg  string `json:"resultMsg"`
		} `json:"header"`
		Body struct {
			Items struct {
				Item interface{} `json:"item"` // 배열 또는 단일 객체
			} `json:"items"`
			NumOfRows  int `json:"numOfRows"`
			PageNo     int `json:"pageNo"`
			TotalCount int `json:"totalCount"`
		} `json:"body"`
	} `json:"response"`
}

// 버스 실시간 위치 정보 구조체 (API 2) - 실제 응답 기준 수정
type BusRealtimeInfo struct {
	GPSLati   float64 `json:"gpslati"`
	GPSLong   float64 `json:"gpslong"`
	NodeID    string  `json:"nodeid,omitempty"` // 선택적 필드
	NodeName  string  `json:"nodenm,omitempty"` // 선택적 필드
	NodeOrd   int     `json:"nodeord"`
	RouteName int     `json:"routenm"`           // int로 고정
	RouteType string  `json:"routetp,omitempty"` // 선택적 필드
	VehicleNo string  `json:"vehicleno"`
}

// RouteName을 문자열로 변환하는 메서드
func (b *BusRealtimeInfo) GetRouteNameString() string {
	if b.RouteName == 0 {
		return ""
	}
	return fmt.Sprintf("%d", b.RouteName)
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

// 통합 버스 위치 정보 구조체 (Elasticsearch용)
type UnifiedBusLocation struct {
	// 공통 필드
	RouteID    string `json:"routeId"`    // 노선 ID
	Timestamp  int64  `json:"timestamp"`  // 타임스탬프 (Unix timestamp)
	DataSource string `json:"dataSource"` // 데이터 소스 (api1_gg_location, api2_kr_realtime)
	VehicleID  string `json:"vehicleId"`  // 차량 ID
	VehicleNo  string `json:"vehicleNo"`  // 차량 번호

	// 운행 상태 (새로 추가)
	OperationStatus string `json:"operationStatus"` // 운행상태 (operating, ending)

	// GPS 좌표 (주로 API 2에서 제공)
	GPSLati float64 `json:"gpsLati"` // 위도
	GPSLong float64 `json:"gpsLong"` // 경도

	// API 1 전용 필드 (경기도 버스 위치 정보)
	StationSeq    int    `json:"stationSeq"`              // 정류소 순번
	Crowded       *int   `json:"crowded,omitempty"`       // 차내혼잡도
	RemainSeatCnt *int   `json:"remainSeatCnt,omitempty"` // 차내빈자리수
	StateCd       *int   `json:"stateCd,omitempty"`       // 상태코드
	LowPlate      *int   `json:"lowPlate,omitempty"`      // 특수차량여부
	TaglessCd     *int   `json:"taglessCd,omitempty"`     // 태그리스 서비스 여부
	RouteTypeCd   *int   `json:"routeTypeCd,omitempty"`   // 노선유형코드
	StationID     *int64 `json:"stationId,omitempty"`     // 정류소 ID

	// API 2 전용 필드 (국토교통부 버스 실시간 위치 정보)
	NodeOrd   int     `json:"nodeOrd"`             // 정류소 순서
	NodeID    *string `json:"nodeId,omitempty"`    // 정류소 ID
	NodeName  *string `json:"nodeName,omitempty"`  // 정류소 이름
	RouteName *string `json:"routeName,omitempty"` // 노선 이름 (문자열로 저장)
	RouteType *string `json:"routeType,omitempty"` // 노선 유형
}

// Elasticsearch 문서 ID 생성
func (u *UnifiedBusLocation) GetDocumentID() string {
	return fmt.Sprintf("%s_%s_%d_%s", u.RouteID, u.VehicleID, u.Timestamp, u.DataSource)
}

// 데이터 검증
func (u *UnifiedBusLocation) IsValid() bool {
	// 필수 필드 검증
	if u.RouteID == "" || u.VehicleID == "" || u.Timestamp == 0 {
		return false
	}

	// 통합 데이터는 항상 유효
	if u.DataSource == "unified" {
		return true
	}

	return false
}

// GPS 좌표 유효성 검증
func (u *UnifiedBusLocation) HasValidGPS() bool {
	return u.GPSLati != 0 && u.GPSLong != 0 &&
		u.GPSLati >= -90 && u.GPSLati <= 90 &&
		u.GPSLong >= -180 && u.GPSLong <= 180
}

// 차내 혼잡도 문자열 변환
func (u *UnifiedBusLocation) GetCrowdedStatus() string {
	if u.Crowded == nil {
		return "정보없음"
	}

	switch *u.Crowded {
	case 1:
		return "여유"
	case 2:
		return "보통"
	case 3:
		return "혼잡"
	case 4:
		return "매우혼잡"
	default:
		return "알 수 없음"
	}
}

// 차량 상태 문자열 변환
func (u *UnifiedBusLocation) GetStateDescription() string {
	if u.StateCd == nil {
		return "정보없음"
	}

	switch *u.StateCd {
	case 0:
		return "교차로통과"
	case 1:
		return "정류소도착"
	case 2:
		return "정류소출발"
	default:
		return "알 수 없음"
	}
}

// JSON 직렬화용 맵 변환 (핵심 필드만)
func (u *UnifiedBusLocation) ToMap() map[string]interface{} {
	result := map[string]interface{}{
		"routeId":         u.RouteID,
		"timestamp":       u.Timestamp,
		"dataSource":      u.DataSource,
		"vehicleId":       u.VehicleID,
		"vehicleNo":       u.VehicleNo,
		"operationStatus": u.OperationStatus,
	}

	// GPS 좌표
	if u.HasValidGPS() {
		result["gpsLati"] = u.GPSLati
		result["gpsLong"] = u.GPSLong
		result["location"] = map[string]interface{}{
			"lat": u.GPSLati,
			"lon": u.GPSLong,
		}
	}

	// 정류장 정보
	if u.NodeOrd > 0 {
		result["nodeOrd"] = u.NodeOrd
	}
	if u.NodeName != nil {
		result["nodeName"] = *u.NodeName
	}
	if u.StationSeq > 0 {
		result["stationSeq"] = u.StationSeq
	}

	// 노선 정보
	if u.RouteName != nil && *u.RouteName != "" {
		result["routeName"] = *u.RouteName
	} else {
		// routeName이 없으면 routeId를 사용
		result["routeName"] = u.RouteID
	}

	if u.RouteType != nil {
		result["routeType"] = *u.RouteType
	}

	// 혼잡도 정보
	if u.Crowded != nil {
		result["crowded"] = *u.Crowded
		result["crowdedStatus"] = u.GetCrowdedStatus()
	}
	if u.RemainSeatCnt != nil {
		result["remainSeatCnt"] = *u.RemainSeatCnt
	}

	// 버스 상태
	if u.StateCd != nil {
		result["stateCd"] = *u.StateCd
		result["stateDescription"] = u.GetStateDescription()
	}

	return result
}
