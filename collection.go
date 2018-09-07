package gofiledb

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"regexp"
	//"github.com/teejays/clog"
	"io"
	"io/ioutil"
	"os"
	//"strconv"
	"strings"
	"sync"
)

/********************************************************************************
* E N T I T I E S
*********************************************************************************/

const (
	ENCODING_NONE uint = iota
	ENCODING_JSON
	ENCODING_GOB
)

type (
	collectionStore struct {
		Store map[string]Collection
		sync.RWMutex
	}

	Collection struct {
		CollectionProps
		DirPath    string
		IndexStore IndexStore
	}

	CollectionProps struct {
		Name                  string
		EncodingType          uint
		EnableGzipCompression bool
		NumPartitions         int
	}
)

var ErrCollectionIsNotExist = fmt.Errorf("Collection not found")
var ErrCollectionIsExist = fmt.Errorf("Collection with this name already exists")

/********************************************************************************
* C O L L E C T I O N  <-> I N D E X
*********************************************************************************/

// fieldLocator could be fieldA.fieldB, Components.Basic.Data.OrgId
func (cl *Collection) addIndex(fieldLocator string) error {

	// check that the index doesn't exist already before
	if cl.isIndexExist(fieldLocator) {
		return ErrIndexIsExist
	}

	// Only enabed JSON indexing
	if cl.EncodingType != ENCODING_JSON {
		return fmt.Errorf("Indexing only supported for JSON encoded data")
	}

	// Go through all the docs in the collection and create the maps...
	idx := NewIndex(cl.Name, fieldLocator, cl.getIndexFilePath(fieldLocator))

	// get path for where all the collection data is
	dataDirPath := joinPath(cl.DirPath, DATA_DIR_NAME)
	err := idx.build(dataDirPath)

	err = idx.save()
	if err != nil {
		return err
	}

	cl.IndexStore.Lock()
	cl.IndexStore.Store[idx.FieldLocator] = idx.IndexInfo
	cl.IndexStore.Unlock()

	return nil

}

func (cl *Collection) getIndexFilePath(fieldLocator string) string {
	return joinPath(cl.getIndexDirPath(), fieldLocator)
}

func (cl *Collection) getIndexDirPath() string {
	return joinPath(cl.DirPath, META_DIR_NAME, "index")
}

func (cl *Collection) addDocToIndexes(k Key) error {

	// get all the indexes
	indexStore := cl.IndexStore.Store

	for fieldLocator := range indexStore {

		idx, err := cl.getIndex(fieldLocator)
		if err != nil {
			return err
		}

		err = idx.addDoc(k, cl.getFilePath(k))
		if err != nil {
			return err
		}

		err = idx.save()
		if err != nil {
			return err
		}

		cl.IndexStore.Lock()
		cl.IndexStore.Store[idx.FieldLocator] = idx.IndexInfo
		cl.IndexStore.Unlock()
	}

	return nil
}

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

	exist := cl.isIndexExist(fieldLocator)
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

func (cl *Collection) isIndexExist(fieldLocator string) bool {
	cl.IndexStore.RLock()
	defer cl.IndexStore.RUnlock()

	_, hasKey := cl.IndexStore.Store[fieldLocator]
	return hasKey
}

/********************************************************************************
* W R I T E R S
*********************************************************************************/

func (cl *Collection) set(k Key, data []byte) error {

	// create the partition dir if it doesn't exist already
	dirPath := joinPath(cl.DirPath, DATA_DIR_NAME, getPartitionDirName(k, cl.NumPartitions))
	err := createDirIfNotExist(dirPath)
	if err != nil {
		return fmt.Errorf("error while creating the dir at path %s: %s", dirPath, err)
	}
	path := cl.getFilePath(k)

	err = ioutil.WriteFile(path, data, FILE_PERM)
	if err != nil {
		return fmt.Errorf("error while writing file: %s", err)
	}

	err = cl.addDocToIndexes(k)
	if err != nil {
		return err
	}

	return nil
}

func (cl *Collection) setFromStruct(k Key, v interface{}) error {

	var data []byte
	var err error

	if cl.EncodingType == ENCODING_JSON {
		data, err = json.Marshal(v)
		if err != nil {
			return err
		}

	} else if cl.EncodingType == ENCODING_GOB {
		var buff bytes.Buffer
		enc := gob.NewEncoder(&buff)
		err = enc.Encode(v)
		if err != nil {
			return err
		}
		data = buff.Bytes()
	}

	return cl.set(k, data)
}

func (cl *Collection) setFromReader(k Key, src io.Reader) error {
	// create the partition dir if it doesn't exist already
	dirPath := joinPath(cl.DirPath, DATA_DIR_NAME, getPartitionDirName(k, cl.NumPartitions))
	err := createDirIfNotExist(dirPath)
	if err != nil {
		return fmt.Errorf("error while creating the dir at path %s: %s", dirPath, err)
	}
	path := cl.getFilePath(k)

	// open the file (copied from https://golang.org/src/io/ioutil/ioutil.go?s=2534:2602#L69)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, src) // first argument is the number of bytes written
	if err != nil {
		return err
	}

	err = cl.addDocToIndexes(k)
	if err != nil {
		return err
	}

	return nil
}

/********************************************************************************
* R E A D E R S
*********************************************************************************/

func (cl *Collection) getFile(k Key) (*os.File, error) {
	path := cl.getFilePath(k)
	return os.Open(path)
}

func (cl *Collection) getFileData(k Key) ([]byte, error) {
	file, err := cl.getFile(k)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := bytes.NewBuffer(nil)

	_, err = io.Copy(buf, file) // the first discarded returnable is the number of bytes copied
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (cl *Collection) getIntoStruct(k Key, dest interface{}) error {

	if cl.EncodingType == ENCODING_JSON {
		data, err := cl.getFileData(k)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, dest)
	}

	if cl.EncodingType == ENCODING_GOB {
		file, err := cl.getFile(k)
		if err != nil {
			return err
		}
		defer file.Close()
		dec := gob.NewDecoder(file)
		return dec.Decode(dest)
	}

	return fmt.Errorf("Encoding logic for the encoding type not implemented")
}

func (cl *Collection) getIntoWriter(k Key, dest io.Writer) error {
	file, err := cl.getFile(k)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(dest, file)
	if err != nil {
		return err
	}

	return nil
}

/********************************************************************************
* O T H E R S
*********************************************************************************/

func (cl *Collection) getFilePath(k Key) string {
	return joinPath(cl.DirPath, DATA_DIR_NAME, getPartitionDirName(k, cl.NumPartitions), cl.getFileName(k))
}

func (cl *Collection) getFileName(k Key) string {
	return cl.Name + "_" + DOC_FILE_NAME_PREFIX + k.String()
}

func (p CollectionProps) sanitize() CollectionProps {
	p.Name = strings.TrimSpace(p.Name)
	p.Name = strings.ToLower(p.Name)

	if p.NumPartitions == 0 { // default value should mean we have one partition
		p.NumPartitions = 1
	}
	return p
}

func (p CollectionProps) validate() error {
	if strings.TrimSpace(p.Name) == "" {
		return fmt.Errorf("Collection name cannot be empty")
	}

	// Special Characters check
	rgx := regexp.MustCompile("[^a-zA-Z0-9]+")
	hasSpecialCharacters := rgx.MatchString(p.Name)
	if hasSpecialCharacters {
		fmt.Errorf("Collection name cannot have any special characters")
	}

	const collectionNameLenMax int = 50
	const collectionNameLenMin int = 2
	if len(p.Name) < collectionNameLenMin {
		fmt.Errorf("Collection name needs to be a minimum of %d chars", collectionNameLenMin)
	}
	if len(p.Name) > collectionNameLenMax {
		fmt.Errorf("Collection name can be a max of %d chars", collectionNameLenMin)
	}

	var supportedEncodings []uint = []uint{ENCODING_NONE, ENCODING_JSON, ENCODING_GOB}
	var isValidEncoding bool
	for _, enc := range supportedEncodings {
		if p.EncodingType == enc {
			isValidEncoding = true
		}
	}
	if !isValidEncoding {
		return fmt.Errorf("Invalid encoding type")
	}

	if p.NumPartitions < 1 {
		return fmt.Errorf("Number of paritions requested can not be negative")
	}

	return nil
}
