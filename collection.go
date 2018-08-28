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
	"sort"
	"strings"
	"sync"
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
		Store map[string]IndexInfo
		sync.RWMutex
	}

	collectionIndexStoreGob struct {
		Store map[string]IndexInfo
	}
)

func (_s CollectionIndexStore) GobEncode() ([]byte, error) {

	s := collectionIndexStoreGob{_s.Store}
	buff := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buff)
	err := enc.Encode(s)
	return buff.Bytes(), err
}

func (_s *CollectionIndexStore) GobDecode([]byte) error {
	var s collectionIndexStoreGob

	buff := bytes.NewBuffer(nil)
	dec := gob.NewDecoder(buff)
	err := dec.Decode(s)
	if err != nil {
		return err
	}
	_s.Store = s.Store
	return nil
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

	// Create the required dir paths for this collection
	cl.DirPath = c.getDirPathForCollection(p.Name)

	// create the dirs for the collection
	err = createDirIfNotExist(joinPath(cl.DirPath, META_DIR_NAME))
	if err != nil {
		return err
	}
	// for indexes
	err = createDirIfNotExist(joinPath(cl.DirPath, META_DIR_NAME, "index"))
	if err != nil {
		return err
	}
	err = createDirIfNotExist(joinPath(cl.DirPath, DATA_DIR_NAME))
	if err != nil {
		return err
	}

	// Initialize the IndexStore, which stores info on the indexes associated with this Collection
	cl.IndexStore.Store = make(map[string]IndexInfo)

	// Register the Collection

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

/*** Navigation Helpers */

func (cl Collection) getFilePath(key string) string {
	return joinPath(cl.DirPath, DATA_DIR_NAME, cl.getPartitionDirName(key), key)
}

func (cl Collection) getPartitionDirName(key string) string {
	h := getPartitionHash(key, cl.NumPartitions)
	return DATA_PARTITION_PREFIX + h
}

/********************************************************************************
* S E A R C H
*********************************************************************************/

type queryPlan struct {
	Query          string
	ConditionsPlan queryConditionsPlan
}

type queryConditionsPlan []queryPlanCondition

type queryPlanCondition struct {
	FieldLocator    string
	ConditionValues []string
	QueryPosition   int
	HasIndex        bool
	IndexInfo       *IndexInfo
}

func (qs queryConditionsPlan) Len() int {
	return len(qs)
}

func (qs queryConditionsPlan) Less(i, j int) bool {
	if qs[i].HasIndex && !qs[j].HasIndex {
		return true
	}
	if !qs[i].HasIndex && qs[j].HasIndex {
		return false
	}
	if qs[i].HasIndex && qs[j].HasIndex {
		return qs[i].IndexInfo.NumValues <= qs[j].IndexInfo.NumValues
	}
	// both don't have indexes, doesn't matter, return something arbitrary e.g. which one was mentioned first in the query
	return qs[i].QueryPosition > qs[j].QueryPosition
}

func (qs queryConditionsPlan) Swap(i, j int) {
	var temp queryPlanCondition = qs[i]
	qs[i] = qs[j]
	qs[j] = temp
}

// Todo: add order by
// e.g query: UserId=1+Org.OrgId=1|261+Name=Talha
func (cl Collection) search(query string) ([]interface{}, error) {
	var err error
	var qPlan queryPlan
	qPlan.Query = query

	// get the plan, which is in the form of type queryConditionsPlan
	qPlan.ConditionsPlan, err = cl.getConditionsPlan(query)
	if err != nil {
		return nil, err
	}

	// execute the plan
	var resultKeys map[string]bool // value type int is just arbitrary so we can store some temp info when find intersects later
	for step, qCondition := range qPlan.ConditionsPlan {
		step++ // so we start with step = 1

		// if index, open index
		if qCondition.HasIndex {
			idx, err := cl.getIndex(qCondition.FieldLocator)
			if err != nil {
				return nil, err
			}

			for _, conditionValue := range qCondition.ConditionValues {

				// for each condition, get the values (doc keys) that satisfy the condition
				docIds := idx.KeyValue[conditionValue]
				if step == 1 {
					// first time we're getting the docs, just add them to results
					for _, dId := range docIds {
						resultKeys[dId] = true
					}

				} else {
					resultKeys = findIntersectionMapSlice(resultKeys, docIds)
				}

			}

		} else { // If there is no index, then we'll have to open all the docs.. :/ Let's not support it for now
			return nil, fmt.Errorf("Searching is only supported on indexed fields. No index found for field %s", qCondition.FieldLocator)

		}

	}

	// After this for loop, we should have a map of all the doc keys we want to return

	var results []interface{}
	for docKey, _ := range resultKeys {
		var doc map[string]interface{}
		err := cl.getIntoStruct(docKey, &doc)
		if err != nil {
			return nil, err
		}
		results = append(results, doc)
	}

	return results, nil

}

// find intersection of a and b
func findIntersectionMapSlice(a map[string]bool, b []string) map[string]bool {

	var intersect map[string]bool = make(map[string]bool)
	// loop through the bs, add them to intersect if they are in a
	for _, bVal := range b {
		if hasKey := a[bVal]; hasKey {
			intersect[bVal] = true
		}
	}

	return intersect
}

// This could be way more advanced, but have to make a call on what functionality to allow right now
// Allowed: ANDs: represented by '+'
func (cl Collection) getConditionsPlan(query string) (queryConditionsPlan, error) {

	var err error
	var qConditionsPlan queryConditionsPlan
	const AND_SEPARATOR string = "+"
	const KV_SEPARATOR string = ":"

	// Split each query by the separator `+`, each part represents a separate conditional
	qParts := strings.Split(query, AND_SEPARATOR)

	// for each of the condition's field locator, we'll get and cache the index info so we don't have to do it again
	var indexInfoCache map[string]IndexInfo = make(map[string]IndexInfo)

	// Each part is a condition statement, euch as UserId=12, OrgId=22.
	for i, qP := range qParts {

		// We need to split it by field locator and the condition value
		// Understand this part of condition
		_qP := strings.SplitN(qP, KV_SEPARATOR, 1)
		if len(_qP) < 2 {
			return qConditionsPlan, fmt.Errorf("Invalid Query around `%s`", qP)
		}
		fieldLocator := _qP[0]
		fieldCondition := _qP[1]

		var qPlanCondition queryPlanCondition
		qPlanCondition.FieldLocator = fieldLocator
		qPlanCondition.ConditionValues = []string{fieldCondition}
		qPlanCondition.QueryPosition = i
		qPlanCondition.HasIndex = cl.indexIsExist(fieldLocator)

		if qPlanCondition.HasIndex {
			idxInfo, inCache := indexInfoCache[fieldLocator]
			if !inCache {
				idxInfo, err = cl.getIndexInfo(fieldLocator)
				if err != nil {
					return qConditionsPlan, err
				}
				indexInfoCache[fieldLocator] = idxInfo
			}

			qPlanCondition.IndexInfo = &idxInfo
		}

		qConditionsPlan = append(qConditionsPlan, qPlanCondition)

	}

	// by this point, we should have info on all conditional statements...
	// we should order the conditionals based on ... 1) if they have index, 2) how big in the index
	// this is done by the sort method
	sort.Sort(qConditionsPlan)

	return qConditionsPlan, nil

}
