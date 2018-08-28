package gofiledb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/teejays/clog"
	"io"
	"os"
	"reflect"
)

type (
	Index struct {
		FieldLocater string
		FieldType    string
		KeyValue     map[string][]string
	}
)

func (cl Collection) getIndex(fieldLocator string) (Index, error) {

	var idx Index

	exist := isIndexExist(fieldLocator)
	if !exist {
		return idx, ErrIndexIsNotExist
	}

	// index exists, so let's read it.
	idxPath := joinPath(cl.DirPath, META_DIR_NAME, "index", fieldLocator)

	file, err := os.Open(idxPath)
	if err != nil {
		return idx, err
	}

	buff := bytes.NewBuffer(nil)
	err = io.Copy(buff, file)
	if err != nil {
		return idx, err
	}

	err = json.Unmarshal(buff, &idx)
	if err != nil {
		return idx, err
	}

	return idx, nil
}

var ErrIndexIsExist error = fmt.Errorf("Index already exists for that field")
var ErrIndexIsNotExist error = fmt.Errorf("Index does not exist")

// fieldName could be fieldA.fieldB, Components.Basic.Data.OrgId
func (cl Collection) addIndex(fieldLocater string) error {

	// check that the index doesn't exist already before
	if cl.IsIndexExist(fieldLocator) {
		return ErrIndexIsExist
	}

	// go through all the docs in the collection and create a map...
	var index Index
	index.FieldLocater = fieldLocater
	index.KeyValue = make(map[string][]string)

	// get path for where all the collection data is
	collDataPath := joinPath(cl.DirPath, DATA_DIR_NAME)

	// open the data dir, which has all the partition dirs
	collectionDataDir, err := os.Open(collDataPath)
	if err != nil {
		return err
	}
	defer collectionDataDir.Close()

	// get all the names of the partition dirs so we can open them
	partitionDirNames, err := collectionDataDir.Readdirnames(-1)
	if err != nil {
		return err
	}

	// for each partition dir, open it, make sures it's a Dir, and get all the files within it.
	for _, pDirName := range partitionDirNames {

		pDirPath := joinPath(collDataPath, pDirName)
		fileInfo, err := os.Stat(pDirPath)
		if err != nil {
			return err
		}
		if !fileInfo.IsDir() {
			clog.Warnf("%s: not a directory", pDirPath)
			continue
		}

		pDir, err := os.Open(pDirPath)
		if err != nil {
			return err
		}
		defer pDir.Close()

		docNames, err := pDir.Readdirnames(-1)
		if err != nil {
			return err
		}

		// open each of the doc, and add it to index
		for _, docName := range docNames {

			docPath := joinPath(pDirPath, docName)

			docFile, err := os.Open(docPath)
			if err != nil {
				return nil
			}
			defer docFile.Close()

			// read the file into a json?
			buff := bytes.NewBuffer(nil)
			_, err = io.Copy(buff, docFile)
			if err != nil {
				return err
			}

			doc := buff.Bytes() // this is the json doc
			var docMap map[string]interface{}

			if cl.EncodingType != ENCODING_JSON {
				return fmt.Errorf("Indexing only supported for JSON data")
			}

			err = json.Unmarshal(doc, &docMap)
			if err != nil {
				return err
			}

			// get the value
			docMap_v := reflect.ValueOf(docMap)
			values, err := GetNestedFieldValues(docMap_v, fieldLocater)
			if err != nil {
				return err
			}

			// each of the 'values' correspond to the value for this doc for the given field
			// we shoud store them in the index
			for _, v := range values {
				// Todo: make sure that the values are hashable (i.e. string, int, float etc. and not map, channels etc.)
				if v.CanInterface() {
					v_i := v.Interface()
					v_str := fmt.Sprintf("%v", v_i)

					index.FieldType = reflect.TypeOf(v_i).Kind().String()
					index.KeyValue[v_str] = append(index.KeyValue[v_str], docName)
				}
			}

		}

	}

	// Save the index file.. but first json encode it
	indexJson, err := json.Marshal(index)
	if err != nil {
		return err
	}

	indexPath := joinPath(cl.DirPath, META_DIR_NAME, "index", fieldLocater)
	indexFile, err := os.Create(indexPath)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	_, err = indexFile.Write(indexJson)
	if err != nil {
		return err
	}

	cl.CollectionStore.Lock()
	cl.CollectionStore.Store[fieldLocator] = true
	cl.CollectionStore.Unock()

	return nil

}

func (cl Collection) isIndexExist(fieldLocator string) (bool, error) {
	cl.CollectionIndexStore.RLock()
	exists := cl.CollectionIndexStore.Store[fieldLocator] // this should return false if the index is not set, or if it the value is set to false
	cl.CollectionIndexStore.RUnlock()
	return exists
}

// Todo: removeIndex()
