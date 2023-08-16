package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type BeginMetadata struct {
	XID       int    `json:"xid"`
	LSN       string `json:"lsn"`
	Timestamp string `json:"timestamp"`
	CommitLSN string `json:"commit_lsn"`
}

type CommitMetadata struct {
	XID       int    `json:"xid"`
	LSN       string `json:"lsn"`
	Timestamp string `json:"timestamp"`
}

func extractJSON(line string) (string, error) {
	startIndex := strings.Index(line, "{")
	endIndex := strings.LastIndex(line, "}")

	if startIndex == -1 || endIndex == -1 || startIndex >= endIndex {
		return "", fmt.Errorf("invalid JSON format")
	}

	return line[startIndex : endIndex+1], nil
}

func GetBeginMetadata(line string) BeginMetadata {
	jsonStr, err := extractJSON(line)
	if err != nil {
		panic(err)
	}
	var beginInfo BeginMetadata
	if err := json.Unmarshal([]byte(jsonStr), &beginInfo); err != nil {
		panic(err)
	}
	return beginInfo
}

func GetCommitMetadata(line string) CommitMetadata {
	jsonStr, err := extractJSON(line)
	if err != nil {
		panic(err)
	}

	var commitInfo CommitMetadata
	if err := json.Unmarshal([]byte(jsonStr), &commitInfo); err != nil {
		panic(err)
	}
	return commitInfo
}
