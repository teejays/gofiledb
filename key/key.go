package key

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	DATA_PARTITION_PREFIX string = "partition_"
	DOC_FILE_NAME_PREFIX  string = "doc_"
)

/********************************************************************************
* K E Y
*********************************************************************************/

type Key int64

func (k Key) String() string {
	return strconv.FormatInt(int64(k), 10)
}

func (k Key) GetPartitionDirName(numPartitions int) string {
	h := k.GetPartitionHash(numPartitions)
	return DATA_PARTITION_PREFIX + h
}

func (k Key) GetPartitionHash(numPartitions int) string {
	return strconv.Itoa(int(k) % numPartitions)
}

func (k Key) GetFileName(collectionName string, enableGzip bool) string {
	fileName := collectionName + "_" + DOC_FILE_NAME_PREFIX + k.String()
	if enableGzip {
		fileName += ".gz"
	}
	return fileName
}

func GetKeyFromFileName(fileName string) (Key, error) {
	var k Key
	parts := strings.Split(fileName, DOC_FILE_NAME_PREFIX)
	if len(parts) != 2 {
		return k, fmt.Errorf("Screw you Talha. Check how you get Key from filenames.")
	}
	keyInt, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return k, err
	}
	k = Key(keyInt)
	return k, nil
}
