package common

import (
	"path/filepath"
	"strings"
)

func ArrayToMap[T string](array []T) map[T]struct{} {
	m := make(map[T]struct{}, len(array))
	for _, t := range array {
		m[t] = struct{}{}
	}
	return m
}

func FileNameWithoutExtension(fileName string) string {
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

func GetFileName(path string) string {
	return filepath.Base(path)
}
