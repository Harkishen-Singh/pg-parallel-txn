package format

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/timescale/promscale/pkg/log"
)

const hypertable_with_id = "SELECT id::BIGINT, schema_name::TEXT, table_name::TEXT FROM _timescaledb_catalog.hypertable ORDER BY 1"
const Extension = ".replayable"

var chunkToHypertable = sync.Map{}

func add(chunkRegex *regexp.Regexp, hypertableWithSchema string) {
	log.Info(
		"msg", "Adding following regex matching",
		"chunk_regex", chunkRegex.String(),
		"hypertable", hypertableWithSchema)
	chunkToHypertable.Store(chunkRegex, hypertableWithSchema)
}

func CompleteMapping(conn *pgx.Conn) error {
	r, err := conn.Query(context.Background(), hypertable_with_id)
	if err != nil {
		return fmt.Errorf("error querying hypertable info: %w", err)
	}
	defer r.Close()
	for r.Next() {
		id := int64(0)
		schemaName := ""
		tableName := ""
		if err := r.Scan(&id, &schemaName, &tableName); err != nil {
			return fmt.Errorf("error scanning results: %w", err)
		}
		re, err := regexp.Compile(fmt.Sprintf(`"_timescaledb_internal"."_hyper_%d_\d+_chunk"`, id))
		if err != nil {
			return fmt.Errorf("error compiling regex: %w", err)
		}
		add(re, fmt.Sprintf(`"%s"."%s"`, schemaName, tableName))
	}
	log.Info("msg", "Completed mapping of chunks to hypertable")
	return nil
}

// Format formats the file.
// The formatting includes converting all _timescaledb_internal._hyper_{}_{}_chunk to <schema_name>.<table_name>
// After formatting completes, the file name is changed to file_name + Extension where the extension is .formatted
// Formatted files are never reformatted.
func Format(file string) (formattedFileName string) {
	log.Debug("msg", "formatting", "file", file)
	start := time.Now()
	formattedFileName = file + Extension
	_, err := os.Stat(formattedFileName)
	if os.IsExist(err) {
		os.Remove(file)
	}

	bSlice, err := os.ReadFile(file)
	if err != nil {
		log.Fatal("msg", "Failed to read the file", "file", file, "err", err.Error())
	}
	content := string(bSlice)
	chunkToHypertable.Range(func(key, value any) bool {
		re := key.(*regexp.Regexp)
		qualified_name := value.(string)
		content = re.ReplaceAllString(string(content), qualified_name)
		return true
	})
	err = os.WriteFile(formattedFileName, []byte(content), 0644)
	if err != nil {
		log.Fatal("msg", "Failed to write the updated content to the file", "file", file, "err", err.Error())
	}
	log.Debug("msg", fmt.Sprintf("transformed file %s", file))
	log.Debug("msg", "formatting complete")
	log.Info("formatting_time", time.Since(start).String())
	return
}
