package main

import (
	"sort"
	"strconv"
)

// sortFilesByName sorts the WAL files using their hexadecimal name.
func sortFilesByName(files []string) ([]string, error) {
	sort.Slice(files, func(i, j int) bool {
		numI, err := strconv.ParseUint(files[i], 16, 64)
		if err != nil {
			return false
		}
		numJ, err := strconv.ParseUint(files[j], 16, 64)
		if err != nil {
			return false
		}
		return numI < numJ
	})

	return files, nil
}
