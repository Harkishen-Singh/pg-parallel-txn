package transform

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/Harkishen-Singh/pg-parallel-txn/common"
	"github.com/Harkishen-Singh/pg-parallel-txn/sort"
	"github.com/timescale/promscale/pkg/log"
)

const transformedFileExtension = ".sql"

func RunTransformRoutine(ctx context.Context, absWalDir string) {
	files, err := os.ReadDir(absWalDir)
	if err != nil {
		log.Fatal("[transform_routine]", "Error reading WAL path", "error", err.Error())
	}

	pending := []string{}
	for _, file := range files {
		if file.Type().IsRegular() && strings.HasSuffix(file.Name(), ".json") {
			fileNameOnly := common.FileNameWithoutExtension(common.GetFileName(file.Name()))
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
		log.Debug("json_to_sql", "no files found to transform")
		return
	}
	log.Info("json_to_sql", "found files to transform", "count", len(pending))
	// Sort the files by their hexadecimal names.
	pending = sort.SortFilesByName(pending)
	for _, f := range pending {
		transform(absWalDir, f)
	}
}

func transform(path, fileName string) {
	tempDir, err := os.MkdirTemp("/tmp", "pgcopydb_transform_temp_*")
	if err != nil {
		panic(err)
	}
	defer os.Remove(tempDir)

	command := fmt.Sprintf(
		"pgcopydb stream transform --dir=%s %s/%s.json %s/%s.sql",
		tempDir,
		path, fileName,
		path, fileName,
	)
	fmt.Println(command)
	log.Debug("[transform_routine]", "transforming", "command", command)

	cmd := exec.Command("pgcopydb", "stream", "transform",
		fmt.Sprintf("--dir=%s", tempDir),
		fmt.Sprintf("%s/%s.json", path, fileName),
		fmt.Sprintf("%s/%s.sql", path, fileName),
	)

	// start the command in a separate process
	err = cmd.Start()
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
