package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestSortFilesByChangeTime(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create 5 files with different creation times
	files := make([]string, 5)
	for i := 0; i < 5; i++ {
		fileName := fmt.Sprintf("%s/file%d.txt", tempDir, len(files)-i-1) // Lexicographically reversed order
		file, err := os.Create(fileName)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		file.Close()

		files[i] = fileName // Store in the correct sorted order

		// Sleep for a second to ensure different creation times
		time.Sleep(1 * time.Second)
	}

	// Call the function to sort the files by creation time
	sortedFiles, err := sortFilesByChangeTime(files)
	if err != nil {
		t.Fatalf("SortFilesByChangeTime failed: %v", err)
	}

	// Verify the sorted order
	for i, file := range sortedFiles {
		if file != files[i] {
			t.Errorf("Expected file at index %d to be %s, but got %s", i, files[i], file)
		}
	}
}
