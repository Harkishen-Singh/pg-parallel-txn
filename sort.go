package main

import (
	"os"
	"sort"
	"syscall"
	"time"
)

func sortFilesByCreationTime(files []string) ([]string, error) {
	type fileWithTime struct {
		name string
		time time.Time
	}

	fileTimes := make([]fileWithTime, len(files))

	for i, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			return nil, err
		}

		statT := info.Sys().(*syscall.Stat_t)
		creationTime := time.Unix(int64(statT.Birthtimespec.Sec), int64(statT.Birthtimespec.Nsec))

		fileTimes[i] = fileWithTime{name: file, time: creationTime}
	}

	sort.Slice(fileTimes, func(i, j int) bool {
		return fileTimes[i].time.Before(fileTimes[j].time)
	})

	sortedFiles := make([]string, len(files))
	for i, fileTime := range fileTimes {
		sortedFiles[i] = fileTime.name
	}

	return sortedFiles, nil
}
