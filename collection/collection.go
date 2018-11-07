package collection

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/teejays/gofiledb/key"
	"github.com/teejays/gofiledb/util"
	"io"
	"io/ioutil"
	"os"
	"regexp"
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

const DATA_DIR_NAME string = "data"
const META_DIR_NAME string = "meta"
const INDEX_DIR_NAME string = "indexes"

type (
	Collection struct {
		DirPath    string
		IndexStore IndexStore
		CollectionProps
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
)

var ErrCollectionIsNotExist = fmt.Errorf("Collection not found")
var ErrCollectionIsExist = fmt.Errorf("Collection with this name already exists")

/********************************************************************************
* W R I T E R S
*********************************************************************************/

func (cl *Collection) Set(k key.Key, data []byte) error {

	// Get the full path for the file & create the partition dir if it doesn't exist already
	dirPath := util.JoinPath(cl.DirPath, DATA_DIR_NAME, k.GetPartitionDirName(cl.NumPartitions))
	err := util.CreateDirIfNotExist(dirPath)
	if err != nil {
		return fmt.Errorf("error while creating the dir at path %s: %s", dirPath, err)
	}
	path := cl.getFilePath(k)

	// If Gzip is enabled, we should gzip compress
	if cl.EnableGzipCompression {

		// Open the file
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		gz := gzip.NewWriter(f)
		gz.Write(data)
		gz.Close()

		if err = f.Close(); err != nil {
			return err
		}

	} else {

		err = ioutil.WriteFile(path, data, util.FILE_PERM)
		if err != nil {
			return fmt.Errorf("error while writing file: %s", err)
		}
	}

	if cl.canIndex() {
		err = cl.addDocToIndexes(k)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cl *Collection) SetFromStruct(k key.Key, v interface{}) error {

	var data []byte
	var err error

	if cl.EncodingType == ENCODING_JSON {
		data, err = json.Marshal(v)
		if err != nil {
			return err
		}

		return cl.Set(k, data)
	}

	return fmt.Errorf("Encoding logic for the encoding type not implemented")
}

// Deprectaing this since this is not very widely used, and difficult to implement with the GZIP compression
// func (cl *Collection) setFromReader(k key.Key, src io.Reader) error {

// 	// create the partition dir if it doesn't exist already
// 	dirPath := util.JoinPath(cl.DirPath, DATA_DIR_NAME, k.GetPartitionDirName(cl.NumPartitions))
// 	err := util.CreateDirIfNotExist(dirPath)
// 	if err != nil {
// 		return fmt.Errorf("error while creating the dir at path %s: %s", dirPath, err)
// 	}
// 	path := cl.getFilePath(k)

// 	// open the file (copied from https://golang.org/src/io/ioutil/ioutil.go?s=2534:2602#L69)
// 	file, err := os.Create(path)
// 	if err != nil {
// 		return err
// 	}

// 	if cl.EnableGzipCompression {
// 		gz := gzip.NewWriter(f)

// 		gz.Write(data)
// 		gz.Close()
// 	}

// 	_, err = io.Copy(file, src) // first argument is the number of bytes written
// 	if err != nil {
// 		return err
// 	}

// 	if cl.canIndex() {
// 		err = cl.addDocToIndexes(k)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

/********************************************************************************
* R E A D E R S
*********************************************************************************/

func (cl *Collection) GetFile(k key.Key) (*os.File, error) {
	path := cl.getFilePath(k)
	return os.Open(path)
}

func (cl *Collection) GetFileData(k key.Key) ([]byte, error) {
	file, err := cl.GetFile(k)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := bytes.NewBuffer(nil)

	if cl.EnableGzipCompression {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		defer gz.Close()

		_, err = io.Copy(buf, gz) // the first discarded returnable is the number of bytes copied
		if err != nil {
			return nil, err
		}

	} else {
		_, err = io.Copy(buf, file) // the first discarded returnable is the number of bytes copied
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (cl *Collection) GetIntoStruct(k key.Key, dest interface{}) error {

	data, err := cl.GetFileData(k)
	if err != nil {
		return err
	}

	if cl.EncodingType == ENCODING_JSON {
		return json.Unmarshal(data, dest)
	}

	return fmt.Errorf("Decoding logic for the encoding type not implemented")
}

// getIntoWriter does not take care of GZIP encoding
func (cl *Collection) GetIntoWriter(k key.Key, dest io.Writer) error {
	file, err := cl.GetFile(k)
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
* C O L L E C T I O N  <-> I N D E X
*********************************************************************************/

func (cl *Collection) canIndex() bool {
	if cl.EncodingType != ENCODING_JSON {
		return false
	}
	return true
}

// fieldLocator could be fieldA.fieldB, Components.Basic.Data.OrgId
func (cl *Collection) AddIndex(fieldLocator string) error {

	// Only enabed JSON indexing
	if cl.EncodingType != ENCODING_JSON {
		return fmt.Errorf("Indexing only supported for JSON encoded data")
	}

	// check that the index doesn't exist already before
	if cl.isIndexExist(fieldLocator) {
		return ErrIndexIsExist
	}

	idx := cl.NewIndex(fieldLocator)

	// Go through all the docs in the collection and create the maps...
	// get path for where all the collection data is
	err := idx.build()

	err = idx.save()
	if err != nil {
		return err
	}

	cl.IndexStore.Lock()
	cl.IndexStore.Store[idx.FieldLocator] = idx.IndexInfo
	cl.IndexStore.Unlock()

	return nil

}

func (cl *Collection) GetDirPathForIndexes() string {
	return util.JoinPath(cl.DirPath, META_DIR_NAME, INDEX_DIR_NAME)
}

// func (cl *Collection) GetDirPathForIndexes() string {
// 	return util.JoinPath(cl.DirPath, META_DIR_NAME, INDEX_DIR_NAME)
// }

func (cl *Collection) addDocToIndexes(k key.Key) error {

	// get all the indexes
	indexStore := cl.IndexStore.Store

	for fieldLocator := range indexStore {

		idx, err := cl.loadIndex(fieldLocator)
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

func (cl *Collection) loadIndex(fieldLocator string) (Index, error) {

	var idx Index

	exist := cl.isIndexExist(fieldLocator)
	if !exist {
		return idx, ErrIndexIsNotExist
	}

	// index exists, so let's read it.
	idxPersistPath := util.JoinPath(cl.GetDirPathForIndexes(), fieldLocator)

	file, err := os.Open(idxPersistPath)
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

	// When we saved (json marshaled) the Index struct, we long the unexported field cl i.e. a pointer to the parent collection.
	// We should therefore put it back when we read (json unmarshal) from disk.
	idx.cl = cl

	return idx, nil
}

func (cl *Collection) isIndexExist(fieldLocator string) bool {
	cl.IndexStore.RLock()
	defer cl.IndexStore.RUnlock()

	_, hasKey := cl.IndexStore.Store[fieldLocator]
	return hasKey
}

/********************************************************************************
* O T H E R S
*********************************************************************************/

func (cl *Collection) getDataPath() string {
	return util.JoinPath(cl.DirPath, DATA_DIR_NAME)
}

func (cl *Collection) getFilePath(k key.Key) string {
	return util.JoinPath(cl.getDataPath(), k.GetPartitionDirName(cl.NumPartitions), k.GetFileName(cl.Name, cl.EnableGzipCompression))
}

/********************************************************************************
* P A R A M S
*********************************************************************************/

func (p CollectionProps) Sanitize() CollectionProps {
	p.Name = strings.TrimSpace(p.Name)
	p.Name = strings.ToLower(p.Name)

	if p.NumPartitions == 0 { // default value should mean we have one partition
		p.NumPartitions = 1
	}
	return p
}

func (p CollectionProps) Validate() error {
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
