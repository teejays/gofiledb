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
		IndexInfo
		KeyValue map[string][]string // Field value -> all the doc keys
		ValueKey map[string][]string // DocKey -> all the field values for it
	}

	IndexInfo struct {
		FieldLocator   string
		FieldType      string
		NumValues      int
		FilePath       string
		CollectionName string
	}
)

func (cl *Collection) getIndexInfo(fieldLocator string) (IndexInfo, error) {

	cl.IndexStore.RLock()
	defer cl.IndexStore.RUnlock()

	indexInfo, hasKey := cl.IndexStore.Store[fieldLocator] // this should return false if the index is not set
	if !hasKey {
		return indexInfo, ErrIndexIsNotExist
	}

	return indexInfo, nil
}

func (cl *Collection) getIndex(fieldLocator string) (Index, error) {

	var idx Index

	exist := cl.indexIsExist(fieldLocator)
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
	_, err = io.Copy(buff, file)
	if err != nil {
		return idx, err
	}

	err = json.Unmarshal(buff.Bytes(), &idx)
	if err != nil {
		return idx, err
	}

	return idx, nil
}

func (cl *Collection) reIndexWithDoc(key string) error {

	// get all the indexes
	indexStore := cl.IndexStore.Store

	for fieldLocator, _ := range indexStore {

		idx, err := cl.getIndex(fieldLocator)
		if err != nil {
			return err
		}

		idx, err = addDocToExistingIndex(idx, key, cl.getFilePath(key))
		if err != nil {
			return err
		}

		err = cl.saveIndex(idx)
		if err != nil {
			return err
		}

	}

	return nil

}

var ErrIndexIsExist error = fmt.Errorf("Index already exists for that field")
var ErrIndexIsNotExist error = fmt.Errorf("Index does not exist")

// fieldName could be fieldA.fieldB, Components.Basic.Data.OrgId
func (cl *Collection) addIndex(fieldLocator string) error {

	// check that the index doesn't exist already before
	if cl.indexIsExist(fieldLocator) {
		return ErrIndexIsExist
	}

	// Only enabed JSON indexing
	if cl.EncodingType != ENCODING_JSON {
		return fmt.Errorf("Indexing only supported for JSON data")
	}

	// go through all the docs in the collection and create a map...
	var index Index
	index.FieldLocator = fieldLocator
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

			index, err = addDocToExistingIndex(index, docName, docPath)
			if err != nil {
				return err
			}
		}
	}

	err = cl.saveIndex(index)
	if err != nil {
		return err
	}

	return nil

}

func (cl *Collection) saveIndex(idx Index) error {

	cl.IndexStore.Lock()
	defer cl.IndexStore.Unlock()

	// Save the index file.. but first json encode it
	idxJson, err := json.Marshal(idx)
	if err != nil {
		return err
	}

	idxPath := joinPath(cl.DirPath, META_DIR_NAME, "index", idx.FieldLocator)
	idxFile, err := os.Create(idxPath)
	if err != nil {
		return err
	}
	defer idxFile.Close()

	_, err = idxFile.Write(idxJson)
	if err != nil {
		return err
	}

	cl.IndexStore.Store[idx.FieldLocator] = idx.IndexInfo

	return nil
}

func addDocToExistingIndex(idx Index, docName, docPath string) (Index, error) {

	docFile, err := os.Open(docPath)
	if err != nil {
		return idx, err
	}
	defer docFile.Close()

	// read the file into a json?
	buff := bytes.NewBuffer(nil)
	_, err = io.Copy(buff, docFile)
	if err != nil {
		return idx, err
	}

	doc := buff.Bytes() // this is the json doc

	var docMap map[string]interface{}
	err = json.Unmarshal(doc, &docMap)
	if err != nil {
		return idx, err
	}

	idx, err = addDataToExistingIndex(idx, docName, docMap)
	if err != nil {
		return idx, err
	}

	return idx, nil
}

func addDataToExistingIndex(idx Index, docName string, docMap map[string]interface{}) (Index, error) {
	// get the value
	docMap_v := reflect.ValueOf(docMap)
	values, err := GetNestedFieldValues(docMap_v, idx.FieldLocator)
	if err != nil {
		return idx, err
	}

	// each of the 'values' correspond to the value for this doc for the given field
	// we shoud store them in the index
	for _, v := range values {
		// Todo: make sure that the values are hashable (i.e. string, int, float etc. and not map, channels etc.)
		if v.CanInterface() {
			v_i := v.Interface()
			v_str := fmt.Sprintf("%v", v_i)

			// theoretically, values that correspond to the provided field locator could be of different types
			// so, if we encounter different types, we should error out
			if idx.FieldType == "" { // if hasn't been set yet, it's probably the first iteration so set it
				idx.FieldType = reflect.TypeOf(v_i).Kind().String()
			}

			// make sure that the field of this value is the same as what we expect
			if idx.FieldType != reflect.TypeOf(v_i).Kind().String() {
				return idx, fmt.Errorf("Field locator %s corresponds to more than one data type. Cannot create an index.", idx.FieldLocator)
			}
			idx.KeyValue[v_str] = append(idx.KeyValue[v_str], docName)

		}
	}

	idx.NumValues = len(idx.KeyValue)

	return idx, nil
}

func (cl *Collection) indexIsExist(fieldLocator string) bool {
	cl.IndexStore.RLock()
	defer cl.IndexStore.RUnlock()

	_, hasKey := cl.IndexStore.Store[fieldLocator] // this should return false if the index is not set
	return hasKey
}

// Todo: removeIndex()
