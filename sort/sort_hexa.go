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
