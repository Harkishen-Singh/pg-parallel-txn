package transform

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Harkishen-Singh/pg-parallel-txn/common"
	"github.com/Harkishen-Singh/pg-parallel-txn/sort"
	"github.com/timescale/promscale/pkg/log"
)

const transformedFileExtension = ".sql"
const checkInterval = 5 * time.Second

func RunTransformRoutine(ctx context.Context, absWalDir string) {
	log.Info("msg", "Starting transform_routine")
	for {
		select {
		case <-ctx.Done():
			log.Info("msg", "Stopping transform_routine")
			return
		default:
		}
		files, err := os.ReadDir(absWalDir)
		if err != nil {
			log.Fatal("msg", "Error reading WAL path", "error", err.Error())
		}

		pending := []string{}
		for _, file := range files {
			if file.Type().IsRegular() && strings.HasSuffix(file.Name(), ".json") {
				fileNameOnly := common.FileNameWithoutExtension(common.GetFileName(file.Name())) // wal_file_name_in_hexadecimal.json
				transformedFileName := fileNameOnly + transformedFileExtension
				exists, err := doesTransformedFileExist(transformedFileName)
				if err != nil {
					log.Error(
						"[transform_routine]", "error scanning transformed file",
						"file", transformedFileName,
						"err", err.Error())
					continue
				}
				if !exists {
					pending = append(pending, fileNameOnly)
				}
			}
		}

		if len(pending) == 0 {
			log.Debug("[transform_routine]", "no files found to scan")
			select {
			case <-time.After(checkInterval):
			case <-ctx.Done():
			}
			continue
		}
		log.Info("[transform_routine]", "found files to scan", "count", len(pending))
		// Sort the files by their hexadecimal names.
		pending = sort.SortFilesByName(pending)
		path := filepath.Dir(absWalDir)
		for _, f := range pending {
			transform(path, f)
		}
	}
}

func transform(path, fileName string) {
	command := fmt.Sprintf(
		"pgcopydb stream transform %s/%s.json %s/%s.sql",
		path, fileName,
		path, fileName,
	)
	log.Debug("[transform_routine]", "transforming", "command", command)
	cmd := exec.Command(command)

	// start the command in a separate process
	err := cmd.Start()
	if err != nil {
		log.Error("[transform_routine]", "error in cmd.Start", "err", err.Error())
		return
	}

	// wait for the command to finish
	err = cmd.Wait()
	if err != nil {
		log.Error("[transform_routine]", "error in cmd.Wait", "err", err.Error())
		return
	}

	if cmd.ProcessState.ExitCode() != 0 {
		log.Error("[transform_routine]", "transformed exited with non-zero code", "code", cmd.ProcessState.ExitCode())
	}
}

func doesTransformedFileExist(f string) (bool, error) {
	_, err := os.Stat(f)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("error when searching for file: %w", err)
}
