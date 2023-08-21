package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/timescale/promscale/pkg/log"
)

const STATE_FILE_NAME = "pg-parallel-txn-state.json"
const STATE_FILE_FORMAT = 1

type state struct {
	Format         int8     `json:"format"`
	CompletedFiles []string `json:"completed_files"`
	CurrentFile    string   `json:"current_file"`
}

func LoadOrCreateState() (*state, error) {
	if _, err := os.Stat(STATE_FILE_NAME); os.IsNotExist(err) {
		// If the file does not exist, initialise one and write it to the file.
		emptyState := &state{
			Format: STATE_FILE_FORMAT,
		}
		data, err := json.Marshal(emptyState)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(STATE_FILE_NAME, data, 0644); err != nil {
			return nil, err
		}
		log.Info("msg", "wrote state file")
		return emptyState, nil
	}

	// If the file exists, read and parse it.
	data, err := os.ReadFile(STATE_FILE_NAME)
	if err != nil {
		return nil, fmt.Errorf("error reading state file: %w", err)
	}
	var loadedState state
	if err := json.Unmarshal(data, &loadedState); err != nil {
		return nil, fmt.Errorf("error unmarshalling state file: %w", err)
	}
	log.Info("msg", "loaded state file")
	return &loadedState, nil
}

func (s *state) MarkCurrentAsComplete() {
	s.CompletedFiles = append(s.CompletedFiles, s.CurrentFile)
	s.CurrentFile = ""
}

func (s *state) Write() error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling json: %w", err)
	}
	return os.WriteFile(STATE_FILE_NAME, data, 0644)
}

func (s *state) UpdateCurrent(name string) {
	s.CurrentFile = name
}
