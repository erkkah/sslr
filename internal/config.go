package sslr

import (
	"encoding/json"
	"io/ioutil"
)

// Config is the core main configuration for SSLR
type Config struct {
	SourceConnection string   `json:"source"`
	TargetConnection string   `json:"target"`
	SourceTables     []string `json:"tables"`
}

// LoadConfig reads a JSON - formatted config file into a Config
func LoadConfig(fileName string) (Config, error) {
	var config Config
	jsonData, err := ioutil.ReadFile(fileName)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(jsonData, &config)
	return config, err
}
