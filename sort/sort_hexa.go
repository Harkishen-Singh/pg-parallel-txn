package sort

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/Harkishen-Singh/pg-parallel-txn/common"
)

// SortFilesByName sorts the WAL files using their hexadecimal name.
func SortFilesByName(absFiles []string) []string {
	filesMap := map[string]string{}
	filesNameOnlySlice := []string{}
	for _, f := range absFiles {
		file_name_only := common.FileNameWithoutExtension((common.GetFileName(f)))
		// Optimization
		// ------------
		// Trim the first 8 digits so that string to uint64 base 16 conversion does not panic due to out of range.
		//
		// Info: The format of wal_file_name in Postgres is a 24-character hexadecimal representation of the LSN.
		// The wal_file_name consists of three parts: TTTTTTTT, XXXXXXXX, and YYYYYYYY.
		// Eg: 0000000200000008000000A0.json is
		// - T (DB cluster timeline ID) 	=> 00000002
		// - X (WAL record position) 		=> 00000008
		// - Z (WAL record position) 		=> 000000A0
		//
		// The first part (T) is the timeline ID, which indicates the history of the database cluster.
		// To avoid overflow, we can ignore this part since the timeline ID is changed only when
		// a point-in-time recovery or a standby promotion is performed. We do not expect to do anything such
		// during the migration.
		//
		// The second and third parts are the high and low 32 bits of the 64-bit segment number, which indicates
		// the position of the WAL record within the WAL stream is the changing part which is what we are concerned with.
		file_name_only = file_name_only[8:]
		filesMap[file_name_only] = f
		filesNameOnlySlice = append(filesNameOnlySlice, file_name_only)
	}

	sort.Slice(filesNameOnlySlice, func(i, j int) bool {
		numI, err := strconv.ParseUint(filesNameOnlySlice[i], 16, 64)
		if err != nil {
			panic(fmt.Sprintf("numI: error while parsing hexadecimal file name: %s", err.Error()))
		}
		numJ, err := strconv.ParseUint(filesNameOnlySlice[j], 16, 64)
		if err != nil {
			panic(fmt.Sprintf("numJ: error while parsing hexadecimal file name: %s", err.Error()))
		}
		return numI < numJ
	})

	// Convert sorted file names to absolute file name.
	result := []string{}
	for _, f := range filesNameOnlySlice {
		result = append(result, filesMap[f])
	}

	return result
}
