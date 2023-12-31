//go:build darwin
// +build darwin

package sort

import (
	"fmt"
	"os"
	st "sort"
	"syscall"
	"time"
)

func SortFilesByChangeTime(files []string) ([]string, error) {
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

		statT, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return nil, fmt.Errorf("failed to cast to syscall.Stat_t")
		}

		changeTime := time.Unix(int64(statT.Ctimespec.Sec), int64(statT.Ctimespec.Nsec))

		fileTimes[i] = fileWithTime{name: file, time: changeTime}
	}

	st.Slice(fileTimes, func(i, j int) bool {
		return fileTimes[i].time.Before(fileTimes[j].time)
	})

	sortedFiles := make([]string, len(files))
	for i, fileTime := range fileTimes {
		sortedFiles[i] = fileTime.name
	}

	return sortedFiles, nil
}
