package sslr

import (
	"encoding/json"
	"io/ioutil"
)

// Config is the core main configuration for SSLR
type Config struct {
	SourceConnection   string   `json:"source"`
	TargetConnection   string   `json:"target"`
	SourceTables       []string `json:"tables"`
	UpdateChunkSize    uint32   `json:"updateChunkSize"`
	DeleteChunkSize    uint32   `json:"deleteChunkSize"`
	MinDeleteChunkSize uint32   `json:"minDeleteChunkSize"`
	ThrottlePercentage float64  `json:"throttlePercentage"`
}

// LoadConfig reads a JSON - formatted config file into a Config
func LoadConfig(fileName string) (Config, error) {
	// Config with default values
	config := Config{
		UpdateChunkSize:    1000,
		ThrottlePercentage: 80,
		DeleteChunkSize:    1000,
		MinDeleteChunkSize: 100,
	}
	jsonData, err := ioutil.ReadFile(fileName)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(jsonData, &config)
	return config, err
}
