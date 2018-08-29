package gofiledb

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/teejays/clog"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

/********************************************************************************
* E N T I T I E S
*********************************************************************************/

var ErrCollectionDoesNotExist = fmt.Errorf("Collection not found")

const (
	ENCODING_NONE uint = iota
	ENCODING_JSON
	ENCODING_GOB
)

type (
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

	IndexStore struct {
		Store map[string]IndexInfo
		sync.RWMutex
	}

	collectionIndexStoreGob struct {
		Store map[string]IndexInfo
	}
)

// CollectionStore has issues when being encoded into Gob, because of the sync.RWMutex
// Therefore, we need to define our own GobEncode/GobDecode functions for it.
func (s *IndexStore) GobEncode() ([]byte, error) {

	_s := collectionIndexStoreGob{s.Store}
	buff := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buff)
	err := enc.Encode(_s)
	return buff.Bytes(), err
}

func (s *IndexStore) GobDecode([]byte) error {
	var s collectionIndexStoreGob

	buff := bytes.NewBuffer(nil)
	dec := gob.NewDecoder(buff)
	err := dec.Decode(s)
	if err != nil {
		return err
	}
	s.Store = _s.Store
	return nil
}

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
	var index Index
	index.FieldLocator = fieldLocator
	index.KeyValue = make(map[string][]string)
	index.ValueKey = make(map[string][]string)

	// get path for where all the collection data is
	dataDirPath := joinPath(cl.DirPath, DATA_DIR_NAME)

	// open the data dir, which has all the partition dirs
	dataDir, err := os.Open(dataDirPath)
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

		pDirPath := joinPath(dataDirPath, pDirName)
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

func (cl *Collection) addDocToIndexes(docKey string) error {

	// get all the indexes
	indexStore := cl.IndexStore.Store

	for fieldLocator := range indexStore {

		idx, err := cl.getIndex(fieldLocator)
		if err != nil {
			return err
		}

		idx, err = idx.addDocByPath(docKey, cl.getFilePath(key))
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

func (cl *Collection) indexIsExist(fieldLocator string) bool {
	cl.IndexStore.RLock()
	defer cl.IndexStore.RUnlock()

	_, hasKey := cl.IndexStore.Store[fieldLocator]
	return hasKey
}

/********************************************************************************
* C L I E N T  <->  C O L L E C T I O N
*********************************************************************************/

func (c *Client) IsCollectionExist(collectionName string) (bool, error) {
	collectionName = strings.TrimSpace(collectionName)
	collectionName = strings.ToLower(collectionName)

	_, err := c.getCollectionByName(collectionName)

	if err == ErrCollectionDoesNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil

}

/********************************************************************************
* W R I T E R S
*********************************************************************************/

func (cl *Collection) set(key string, data []byte) error {

	// create the partition dir if it doesn't exist already
	dirPath := joinPath(cl.DirPath, DATA_DIR_NAME, cl.getPartitionDirName(key))
	err := createDirIfNotExist(dirPath)
	if err != nil {
		return fmt.Errorf("error while creating the dir at path %s: %s", dirPath, err)
	}
	path := cl.getFilePath(key)

	err = ioutil.WriteFile(path, data, FILE_PERM)
	if err != nil {
		return fmt.Errorf("error while writing file: %s", err)
	}

	err = cl.reIndexWithDoc(key)
	if err != nil {
		return err
	}

	return nil
}

func (cl *Collection) setFromStruct(key string, v interface{}) error {

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

	return cl.set(key, data)
}

func (cl *Collection) setFromReader(key string, src io.Reader) error {
	// create the partition dir if it doesn't exist already
	dirPath := joinPath(cl.DirPath, DATA_DIR_NAME, cl.getPartitionDirName(key))
	err := createDirIfNotExist(dirPath)
	if err != nil {
		return fmt.Errorf("error while creating the dir at path %s: %s", dirPath, err)
	}
	path := cl.getFilePath(key)

	// open the file (copied from https://golang.org/src/io/ioutil/ioutil.go?s=2534:2602#L69)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, src) // first argument is the number of bytes written
	if err != nil {
		return err
	}

	err = cl.reIndexWithDoc(key)
	if err != nil {
		return err
	}

	return nil
}

/********************************************************************************
* R E A D E R S
*********************************************************************************/

func (cl *Collection) getFile(key string) (*os.File, error) {
	path := cl.getFilePath(key)
	return os.Open(path)
}

func (cl *Collection) getFileData(key string) ([]byte, error) {
	file, err := cl.getFile(key)
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

func (cl *Collection) getIntoStruct(key string, dest interface{}) error {

	if cl.EncodingType == ENCODING_JSON {
		data, err := cl.getFileData(key)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, dest)
	}

	if cl.EncodingType == ENCODING_GOB {
		file, err := cl.getFile(key)
		if err != nil {
			return err
		}
		defer file.Close()
		dec := gob.NewDecoder(file)
		return dec.Decode(dest)
	}

	return fmt.Errorf("Encoding logic for the encoding type not implemented")
}

func (cl *Collection) getIntoWriter(key string, dest io.Writer) error {
	file, err := cl.getFile(key)
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

func (cl *Collection) getFilePath(key string) string {
	return joinPath(cl.DirPath, DATA_DIR_NAME, cl.getPartitionDirName(key), key)
}

func (cl *Collection) getPartitionDirName(key string) string {
	h := getPartitionHash(key, cl.NumPartitions)
	return DATA_PARTITION_PREFIX + h
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
