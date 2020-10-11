package sslr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"time"
)

// Config is the main configuration for SSLR
type Config struct {
	SourceConnection     string   `json:"source"`
	TargetConnection     string   `json:"target"`
	SourceTables         []string `json:"tables"`
	FilteredSourceTables map[string]struct {
		Where  string   `json:"where"`
		Wheres []string `json:"wheres"`
		Uses   []string `json:"uses"`
	} `json:"filteredTables"`
	UpdateChunkSize      uint32        `json:"updateChunkSize"`
	DeleteChunkSize      uint32        `json:"deleteChunkSize"`
	MinDeleteChunkSize   uint32        `json:"minDeleteChunkSize"`
	ThrottlePercentage   float64       `json:"throttlePercentage"`
	StateTableName       string        `json:"stateTable"`
	SyncUpdates          bool          `json:"syncUpdates"`
	SyncDeletes          bool          `json:"syncDeletes"`
	ResyncOnSchemaChange bool          `json:"resyncOnSchemaChange"`
	FullCopyThreshold    float64       `json:"fullCopyThreshold"`
	WaitBetweenJobs      time.Duration `json:"waitBetweenJobs"`
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
		WaitBetweenJobs:      time.Second * 5,
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

	var parsed map[string]interface{}
	err := json.Unmarshal(jsonData, &parsed)
	if err != nil {
		return err
	}

	validField := func(field string, validType reflect.Type) bool {
		numFields := validType.NumField()

		// Comment hack
		if strings.HasPrefix(field, "/*") {
			return true
		}
		for i := 0; i < numFields; i++ {
			tag := validType.Field(i).Tag
			value, ok := tag.Lookup("json")
			if ok && value == field {
				return true
			}
		}
		return false
	}

	template := Config{}
	for k := range parsed {
		if !validField(k, reflect.TypeOf(template)) {
			return fmt.Errorf("Unknown setting %q", k)
		}
	}

	if filtered, ok := parsed["filteredTables"]; ok {
		for _, v := range filtered.(map[string]interface{}) {
			entry := v.(map[string]interface{})
			filteredType := reflect.TypeOf(template.FilteredSourceTables)
			for k := range entry {
				if !validField(k, filteredType.Elem()) {
					return fmt.Errorf("Unknown filtered table setting %q", k)
				}
			}
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

	for table, settings := range cfg.FilteredSourceTables {
		for _, used := range settings.Uses {
			if !hasTable(used) {
				return fmt.Errorf("unknown table %q in uses list", used)
			}
		}
		if len(settings.Wheres) > 0 {
			if len(settings.Where) > 0 {
				return fmt.Errorf("cannot set both 'where' and 'wheres' for table %q", table)
			}
			settings.Where = strings.Join(settings.Wheres, " ")
			cfg.FilteredSourceTables[table] = settings
		}
	}

	return nil
}
