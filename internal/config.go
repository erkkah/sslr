package sslr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
)

// Config is the main configuration for SSLR
type Config struct {
	SourceConnection     string   `json:"source"`
	TargetConnection     string   `json:"target"`
	SourceTables         []string `json:"tables"`
	FilteredSourceTables map[string]struct {
		Where string   `json:"where"`
		Uses  []string `json:"uses"`
	} `json:"filteredTables"`
	UpdateChunkSize      uint32  `json:"updateChunkSize"`
	DeleteChunkSize      uint32  `json:"deleteChunkSize"`
	MinDeleteChunkSize   uint32  `json:"minDeleteChunkSize"`
	ThrottlePercentage   float64 `json:"throttlePercentage"`
	StateTableName       string  `json:"stateTable"`
	SyncUpdates          bool    `json:"syncUpdates"`
	SyncDeletes          bool    `json:"syncDeletes"`
	ResyncOnSchemaChange bool    `json:"resyncOnSchemaChange"`
	FullCopyThreshold    float64 `json:"fullCopyThreshold"`
}

// LoadConfig reads a JSON - formatted config file into a Config.
// The loaded JSON is validated against the expected fields.
func LoadConfig(fileName string) (Config, error) {
	// Config with default values
	config := Config{
		UpdateChunkSize:      1000,
		ThrottlePercentage:   80,
		DeleteChunkSize:      1000,
		MinDeleteChunkSize:   100,
		StateTableName:       "__sslr_state",
		SyncUpdates:          true,
		SyncDeletes:          true,
		ResyncOnSchemaChange: false,
		FullCopyThreshold:    0.5,
	}
	jsonData, err := ioutil.ReadFile(fileName)
	if err != nil {
		return config, err
	}
	err = validateSource(jsonData)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		return config, err
	}

	err = config.validateUses()
	if err != nil {
		return config, err
	}

	return config, nil
}

func validateSource(jsonData []byte) error {
	configType := reflect.TypeOf(Config{})
	var parsed map[string]interface{}
	err := json.Unmarshal(jsonData, &parsed)
	if err != nil {
		return err
	}

	numFields := configType.NumField()
	validField := func(field string) bool {
		// Comment hack
		if strings.HasPrefix(field, "/*") {
			return true
		}
		for i := 0; i < numFields; i++ {
			tag := configType.Field(i).Tag
			value, ok := tag.Lookup("json")
			if ok && value == field {
				return true
			}
		}
		return false
	}

	for k := range parsed {
		if !validField(k) {
			return fmt.Errorf("Unknown setting %q", k)
		}
	}

	return nil
}

func (cfg Config) validateUses() error {
	hasTable := func(needle string) bool {
		for _, table := range cfg.SourceTables {
			if table == needle {
				return true
			}
		}

		for table := range cfg.FilteredSourceTables {
			if table == needle {
				return true
			}
		}

		return false
	}

	for _, settings := range cfg.FilteredSourceTables {
		for _, used := range settings.Uses {
			if !hasTable(used) {
				return fmt.Errorf("unknown table %q in uses list", used)
			}
		}
	}

	return nil
}
