package util

import (
	"github.com/teejays/clog"
	"os"
	"strings"
)

const (
	DATA_DIR_NAME string = "data"
	META_DIR_NAME string = "meta"

	FILE_PERM = 0660
	DIR_PERM  = 0750
)

/********************************************************************************
* H E L P E R 																	*
*********************************************************************************/

func JoinPath(dirs ...string) string {
	return strings.Join(dirs, string(os.PathSeparator))
}

func CreateDirIfNotExist(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		clog.Infof("[GoFileDB] Creating dir at: %s", path)
		err := os.MkdirAll(path, DIR_PERM)
		if err != nil {
			return nil
		}
	}
	return nil
}
