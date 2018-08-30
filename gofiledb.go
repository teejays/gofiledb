// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	DATA_DIR_NAME string = "data"
	META_DIR_NAME string = "meta"

	DATA_PARTITION_PREFIX string = "partition_"
	DOC_FILE_NAME_PREFIX  string = "doc_"

	FILE_PERM = 0660
	DIR_PERM  = 0750
)

type Key int64

// type DocWrapper struct {
// 	Key Key
// 	Doc interface{}
// }

func (k Key) String() string {
	// return string(k)
	return strconv.FormatInt(int64(k), 10)
}

/********************************************************************************
* H E L P E R 																	*
*********************************************************************************/

func joinPath(dirs ...string) string {
	return strings.Join(dirs, string(os.PathSeparator))
}

func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

func createDirIfNotExist(path string) error {

	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, DIR_PERM)
		if err != nil {
			return nil
		}
	}
	return nil
}

func getFileJson(path string) (map[string]interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// read the file into a json?
	buff := bytes.NewBuffer(nil)
	_, err = io.Copy(buff, file)
	if err != nil {
		return nil, err
	}

	doc := buff.Bytes() // this is the json doc

	var data map[string]interface{}
	err = json.Unmarshal(doc, &data)

	return data, err
}

/********************************************************************************
* P A R T I T I O N I N G
*********************************************************************************/
// This section is used to spread files across multiple directories (so one folder doesn't end up with too many files).

/* This function takes a string, convert each byte to a number representation and adds it, then returns a mod */
func getPartitionHash(str string, modConstant int) string {
	var sum int
	for i := 0; i < len(str); i++ {
		sum += int(str[i])
	}
	return strconv.Itoa(sum % modConstant)
}

// func getFileNameFromKey(k Key) string {
// 	return DOC_FILE_NAME_PREFIX + k.String()
// }

/********************************************************************************
* Collection Store
*********************************************************************************/

// func (c *collectionStore) save() error {
// 	c.Lock()
// 	defer c.Unlock()

// 	// get the path using the globally available client (client) variable
// 	path := (&client).getDocumentRoot()
// 	path = path + string(os.PathSeparator) + "db_meta"
// 	err := createDirIfNotExist(path)
// 	if err != nil {
// 		return err
// 	}

// 	// open the file where to save
// 	path = path + string(os.PathSeparator) + "registered_collections.gob"
// 	file, err := os.Create(path)
// 	if err != nil {
// 		return err
// 	}

// 	// write Gob encoded data into the file
// 	encoder := gob.NewEncoder(file)
// 	err = encoder.Encode(c)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func loadRegisteredCollectionsMeta() (collectionStore error {
// 	c.Lock()
// 	defer c.Unlock()

// 	// get the path using the globally available client (client) variable
// 	path := (&client).getDocumentRoot()
// 	path = path + string(os.PathSeparator) + "db_meta"

// 	// open the file where to save
// 	path = path + string(os.PathSeparator) + "registered_collections.gob"
// 	file, err := os.Open(path)
// 	if err != nil {
// 		return err
// 	}

// 	// write Gob encoded data into the file
// 	decoder := gob.NewDecoder(file)
// 	err = decoder.Decode(c)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }
