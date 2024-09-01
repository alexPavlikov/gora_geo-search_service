package models

type Cord struct {
	DriverID  int     `json:"driver_id"`
	Latitude  float32 `json:"latitude"`
	Longitude float32 `json:"longitude"`
}
