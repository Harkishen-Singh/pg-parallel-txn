package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// CommitInfo represents the structure of the JSON data
type CommitInfo struct {
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

func GetCommitInfoFromBeginStmt(line string) (CommitInfo, error) {
	jsonStr, err := extractJSON(line)
	if err != nil {
		panic(err)
	}

	var commitInfo CommitInfo
	if err := json.Unmarshal([]byte(jsonStr), &commitInfo); err != nil {
		panic(err)
	}
	return commitInfo, nil
}
