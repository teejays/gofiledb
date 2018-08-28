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
)

/********************************************************************************
* C O L L E C T I O N
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
		IndexStore CollectionIndexStore
	}

	CollectionProps struct {
		Name                  string
		EncodingType          uint
		EnableGzipCompression bool
		NumPartitions         int
	}

	CollectionIndexStore struct {
		Store map[string]bool
		sync.Mutex
	}
)

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

func (c *Client) AddCollection(p CollectionProps) error {

	// Sanitize the collection props
	p = p.sanitize()

	// Validate the collection props
	err := p.validate()
	if err != nil {
		return err
	}

	// Create a Colelction and add to registered collections
	var cl Collection
	cl.CollectionProps = p

	// Don't repeat collection names
	c.registeredCollections.RLock()
	_, hasKey := c.registeredCollections.Store[p.Name]
	c.registeredCollections.RUnlock()
	if hasKey {
		return fmt.Errorf("A collection with name %s already exists", p.Name)
	}

	// calculate the dir path for this collection
	cl.DirPath = c.getDirPathForCollection(p.Name)

	// create the dirs for the collection
	err = createDirIfNotExist(joinPath(cl.DirPath, META_DIR_NAME))
	if err != nil {
		return err
	}
	err = createDirIfNotExist(joinPath(cl.DirPath, DATA_DIR_NAME))
	if err != nil {
		return err
	}

	c.registeredCollections.Lock()
	defer c.registeredCollections.Unlock()

	// Initialize the collection store if not initialized (but it should already be initialized because of the Initialize() function)
	if c.registeredCollections.Store == nil {
		c.registeredCollections.Store = make(map[string]Collection)
	}
	c.registeredCollections.Store[p.Name] = cl

	err = c.setGlobalMetaStruct("registered_collections.gob", c.registeredCollections.Store)
	if err != nil {
		return err
	}

	return nil
}

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

func (c *Client) RemoveCollection(collectionName string) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	// Delete all the data & meta dirs for that collection
	clog.Infof("Deleting data at %s...", cl.DirPath)
	err = os.RemoveAll(cl.DirPath)
	if err != nil {
		return err
	}

	// remove the reference in the registration store

	c.registeredCollections.Lock()
	defer c.registeredCollections.Unlock()

	clog.Infof("Removing collection registration...")

	delete(c.registeredCollections.Store, collectionName)
	err = c.setGlobalMetaStruct("registered_collections.gob", c.registeredCollections.Store)
	if err != nil {
		return err
	}

	return nil
}

/*** Data Writers */

func (cl Collection) set(key string, data []byte) error {

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

	return nil
}

func (cl Collection) setFromStruct(key string, v interface{}) error {

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

func (cl Collection) setFromReader(key string, src io.Reader) error {
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

	return nil
}

/*** Data Readers */

func (cl Collection) getFile(key string) (*os.File, error) {
	path := cl.getFilePath(key)
	return os.Open(path)
}

func (cl Collection) getFileData(key string) ([]byte, error) {
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

func (cl Collection) getIntoStruct(key string, dest interface{}) error {

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

func (cl Collection) getIntoWriter(key string, dest io.Writer) error {
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

/*** Searchers ***/

type queryPlan struct {
	Query string
	Plan []queryPlanField
}

type queryPlanField struct {
	FieldLocator  string
	Conditions    []string
	QueryPosition int
	HasIndex      bool
}

// Todo: add order by
// e.g query: UserId=1+Org.OrgId=1|261+Name=Talha
func (cl Collection) search(query string) ([]interface{}, error) {
	var qPlan queryPlan
	qPlan = q

	// understand the query
	// split by "+"
	qParts := strings.Split(query, "+") // qParts generally represent the WHERE conditions

	qPlan.Plan = make([]queryPlanField, len(qParts))

	// Plan queue
	// each part should probaby start by fieldLocator
	// let's assume we only support search by value per fieldLocator and not anything fancy like "OR", "AND" greater than, less then etc.
	for i, qP := range qParts {
		_qPart := strings.SplitN(qP, "=", 1)
		if len(_qPart) < 2 {
			return nil, fmt.Errorf("Invalid Query around `%s`", qPart)
		}
		fieldLocator := _qpart[0]
		fieldCondition := _qPart[1]

		var qPlanField queryPlanField
		qPlanField.FieldLocator = fieldLocator
		qPlanField.Conditions = []string{fieldCondition}
		qPlanField.QueryPosition = i
		qPlanField.HasIndex = cl.isIndexExist(fieldLocator)

		// if there is an index, we should probably prioritize it
		... 

	}

	// get the keys of docs that satisfy the conditions

	// if there is an index,
}

/*** Navigation Helpers */

func (cl Collection) getFilePath(key string) string {
	return joinPath(cl.DirPath, DATA_DIR_NAME, cl.getPartitionDirName(key), key)
}

func (cl Collection) getPartitionDirName(key string) string {
	h := getPartitionHash(key, cl.NumPartitions)
	return DATA_PARTITION_PREFIX + h
}
