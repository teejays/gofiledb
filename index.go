package gofiledb

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/teejays/clog"
	"os"
	"reflect"
	"sync"
)

type (
	IndexStore struct {
		Store map[string]IndexInfo
		sync.RWMutex
	}

	Index struct {
		IndexInfo
		ValueKeys map[string][]Key // Field value -> all the doc keys
		KeyValues map[Key][]string // DocKey -> all the field values for it (useful when re-indexing...)
	}

	IndexInfo struct {
		CollectionName string
		FieldLocator   string
		FieldType      string
		NumValues      int
		FilePath       string
	}

	IndexStoreGobFriendly struct {
		Store map[string]IndexInfo
	}
)

// CollectionStore has issues when being encoded into Gob, because of the sync.RWMutex
// Therefore, we need to define our own GobEncode/GobDecode functions for it.
func (s IndexStore) GobEncode() ([]byte, error) {

	_s := IndexStoreGobFriendly{s.Store}
	buff := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buff)
	err := enc.Encode(_s)
	return buff.Bytes(), err
}

func (s *IndexStore) GobDecode(b []byte) error {
	var _s IndexStoreGobFriendly

	buff := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buff)
	err := dec.Decode(_s)
	if err != nil {
		return err
	}
	s.Store = _s.Store
	return nil
}

var ErrIndexIsExist error = fmt.Errorf("Index already exists")
var ErrIndexIsNotExist error = fmt.Errorf("Index does not exist")

func NewIndex(collectionName string, fieldLocator string, filePath string) *Index {
	var idx Index

	idx.CollectionName = collectionName
	idx.FieldLocator = fieldLocator
	idx.FilePath = filePath
	idx.ValueKeys = make(map[string][]Key)
	idx.KeyValues = make(map[Key][]string)

	return &idx

}

func (idx *Index) build(dataPath string) error {

	// open the data dir, which has all the partition dirs
	dataDir, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	defer dataDir.Close()

	// get all the names of the partition dirs so we can open them
	partitionDirNames, err := dataDir.Readdirnames(-1)
	if err != nil {
		return err
	}

	// for each partition dir, open it, make sures it's a Dir, and get all the files within it.
	for _, pDirName := range partitionDirNames {

		pDirPath := joinPath(dataPath, pDirName)
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

			key, err := getKeyFromFileName(docName)
			if err != nil {
				return err
			}
			err = idx.addDoc(key, docPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
func (idx *Index) addDocDir(path string) error {

	fileInfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("directory not found at %s", path)
	}

	pDir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer pDir.Close()

	docNames, err := pDir.Readdirnames(-1)
	if err != nil {
		pDir.Close()
		return err
	}

	// Close the directory since we've read the file names
	err = pDir.Close()
	if err != nil {
		return err
	}

	// open each of the doc, and add it to index
	for _, docName := range docNames {

		docPath := joinPath(path, docName)

		key, err := getKeyFromFileName(docName)
		if err != nil {
			return err
		}

		err = idx.addDoc(key, docPath)
		if err != nil {
			return err
		}
	}

	return nil
}
func (idx *Index) addDoc(k Key, path string) error {

	data, err := getFileJson(path)
	if err != nil {
		return nil
	}

	err = idx.addData(k, data)
	if err != nil {
		return err
	}

	return nil
}

func (idx *Index) addData(k Key, data map[string]interface{}) error {

	// Remove the existing data in the index for this Key
	// Remove the data from the ValueKeys map
	oldValues := idx.KeyValues[k]
	for _, v := range oldValues {
		for i, _k := range idx.ValueKeys[v] {
			if _k == k {
				idx.ValueKeys[v] = append(idx.ValueKeys[v][:i], idx.ValueKeys[v][i+1:]...)
			}
		}
	}
	// Reset the KeyValues Map for k
	idx.KeyValues[k] = []string{}

	// Get the field values
	values, err := GetNestedFieldValuesOfStruct(data, idx.FieldLocator)
	if err != nil {
		return err
	}

	// Each of the 'values' correspond to the value for this doc for the given field
	// we shoud store them in the index
	for _, v := range values {
		// Todo: make sure that the values are hashable (i.e. string, int, float etc. and not map, channels etc.)?
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
				return fmt.Errorf("Field locator %s corresponds to more than one data type. Cannot create an index.", idx.FieldLocator)
			}
			// add values to maps
			idx.ValueKeys[v_str] = append(idx.ValueKeys[v_str], k)
			idx.KeyValues[k] = append(idx.KeyValues[k], v_str)

		}
	}

	idx.NumValues = len(idx.ValueKeys)

	return nil
}

func (idx *Index) save() error {
	// Save the index file.. but first json encode it
	idxJson, err := json.Marshal(idx)
	if err != nil {
		return err
	}

	idxFile, err := os.Create(idx.FilePath)
	if err != nil {
		return err
	}
	defer idxFile.Close()

	_, err = idxFile.Write(idxJson)
	if err != nil {
		return err
	}

	return nil
}
