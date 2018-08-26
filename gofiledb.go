// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"encoding/gob"

	"fmt"
	"io"

	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	DATA_DIR_NAME string = "data"
	META_DIR_NAME string = "meta"

	DATA_PARTITION_PREFIX string = "partition_"

	FILE_PERM = 0660
	DIR_PERM  = 0750
)

// Client is the primary object that the application interacts with while saving or fetching data
type (
	Client struct {
		ClientParams
		RegisteredCollections collectionStore
		isInitialized         bool // IsInitialized ensures that we don't initialize the client more than once, since doing that could lead to issues
		sync.RWMutex
	}

	ClientParams struct {
		documentRoot  string // DocumentRoot is the absolute path to the directory that can be used for storing the files/data
		numPartitions int    // NumPartitions determines how many sub-folders should the package create inorder to partition the data
		enableGzip    bool
	}
)

func NewClientParams(documentRoot string, numPartitions int) ClientParams {
	var params ClientParams = ClientParams{
		documentRoot:  documentRoot,
		numPartitions: numPartitions,
	}
	return params
}

// client is the instance of the Client struct
var client Client

// GetClient returns the current instance of the client for the application. It panics if the client has not been initialized.
func GetClient() *Client {
	if !(&client).isInitialized {
		log.Fatal("GoFiledb client fetched called without initializing the client")
	}
	return &client
}

// InitClient setsup the package for use by an appliction. This should be called before the client can be used.
func Initialize(p ClientParams) error {
	// Although rare, it is still possible that two almost simultaneous calls are made to the Initialize function,
	// which could end up initializing the client twice and might overwrite the param values. Hence, we use a lock
	// to avoid that situation.
	(&client).Lock()
	defer (&client).Unlock()

	if client.isInitialized {
		return fmt.Errorf("GoFileDb client attempted to initialize more than once")
	}

	// Ensure that the params provided make sense
	err := p.validate()
	if err != nil {
		return err
	}

	// Sanitize the params so they'r emore standard
	p = p.sanitize()

	// Set the client
	client.ClientParams = p

	// Initialize the CollectionStore
	err = client.RegisteredCollections.load()
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	client.isInitialized = true

	return nil
}

func (p ClientParams) validate() error {
	// documentRoot shall not be totally white
	if strings.TrimSpace(p.documentRoot) == "" {
		return fmt.Errorf("Empty documentRoot field provided")
	}
	// numPartitions shall be positive
	if p.numPartitions < 1 {
		return fmt.Errorf("Invalid numPartitions value provided: %d", p.numPartitions)
	}
	// documentRoot shall exist as a directory
	info, err := os.Stat(p.documentRoot)
	if os.IsNotExist(err) {
		return fmt.Errorf("no directory found at path %s", p.documentRoot)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s path is not a directory", p.documentRoot)
	}

	return nil
}

func (p ClientParams) sanitize() ClientParams {

	// remove trailing path separator characters (e.g. / in Linux) from the documentRoot
	if len(p.documentRoot) > 0 && p.documentRoot[len(p.documentRoot)-1] == os.PathSeparator {
		p.documentRoot = p.documentRoot[:len(p.documentRoot)-1]
		return p.sanitize()
	}

	// create a new folder at the path provided
	p.documentRoot = p.documentRoot + string(os.PathSeparator) + "gofiledb_warehouse"

	return p

}

func (c *Client) getDocumentRoot() string {
	return c.documentRoot
}

func (c *Client) getCollectionByName(collectionName string) (Collection, error) {
	c.RegisteredCollections.RLock()
	defer c.RegisteredCollections.RUnlock()

	//fmt.Printf("RegisteredCollections: /n %v\n", c.RegisteredCollections)
	coll, hasKey := c.RegisteredCollections.Store[collectionName]
	if !hasKey {
		return coll, ErrCollectionDoesNotExist
	}
	return coll, nil
}

func (c *Client) AddIndex(collectionName string, fieldLocator string) error {
	coll, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return coll.AddIndex(fieldLocator)
}

/********************************************************************************
* W R I T E
*********************************************************************************/

func (c *Client) Set(collectionName string, key string, data []byte) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.set(key, data)
}

func (c *Client) SetStruct(collectionName string, key string, v interface{}) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.setFromStruct(key, v)
}

func (c *Client) SetFromReader(collectionName, key string, src io.Reader) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.setFromReader(key, src)
}

/********************************************************************************
* R E A D 																		*
*********************************************************************************/

func (c *Client) GetFile(collectionName, key string) (*os.File, error) {
	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return nil, err
	}

	return cl.getFile(key)
}

func (c *Client) Get(collectionName string, key string) ([]byte, error) {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return nil, err
	}

	return cl.getFileData(key)
}

func (c *Client) GetIfExist(collectionName string, key string) ([]byte, error) {

	data, err := c.Get(collectionName, key)
	if os.IsNotExist(err) { // if doesn't exist, return nil
		return nil, nil
	}
	return data, err
}

func (c *Client) GetStruct(collectionName string, key string, dest interface{}) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.getIntoStruct(key, dest)
}

func (c *Client) GetStructIfExists(collectionName string, key string, dest interface{}) error {

	err := c.GetStruct(collectionName, key, dest)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (c *Client) GetIntoWriter(collectionName, key string, dest io.Writer) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}
	return cl.getIntoWriter(key, dest)
}

/********************************************************************************
* H E L P E R 																	*
*********************************************************************************/

func (c *Client) getFilePath(collectionName, key string) string {
	return c.getDirPathForData(collectionName, key) + string(os.PathSeparator) + key
}

func (c *Client) getDirPathForData(collectionName, key string) string {
	collectionDirPath := c.getDirPathForCollection(collectionName)
	dirs := []string{collectionDirPath, DATA_DIR_NAME, c.getPartitionDirName(key)}
	return strings.Join(dirs, string(os.PathSeparator))
}

func (c *Client) getDirPathForCollection(collectionName string) string {
	dirs := []string{c.documentRoot, DATA_DIR_NAME, collectionName}
	return strings.Join(dirs, string(os.PathSeparator))
}

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

func (c *Client) FlushAll() error {
	return os.RemoveAll(c.documentRoot)
}

/********************************************************************************
* P A R T I T I O N I N G
*********************************************************************************/
// This section is used to spread files across multiple directories (so one folder doesn't end up with too many files).

func (c *Client) getPartitionDirName(key string) string {
	h := getPartitionHash(key, c.numPartitions)
	return DATA_PARTITION_PREFIX + h
}

/* This function takes a string, convert each byte to a number representation and adds it, then returns a mod */
func getPartitionHash(str string, modConstant int) string {
	var sum int
	for i := 0; i < len(str); i++ {
		sum += int(str[i])
	}
	return strconv.Itoa(sum % modConstant)
}

type collectionStore struct {
	Store map[string]Collection
	sync.RWMutex
}

func (c *collectionStore) save() error {
	c.Lock()
	defer c.Unlock()

	// get the path using the globally available client (client) variable
	path := (&client).getDocumentRoot()
	path = path + string(os.PathSeparator) + "db_meta"
	err := createDirIfNotExist(path)
	if err != nil {
		return err
	}

	// open the file where to save
	path = path + string(os.PathSeparator) + "registered_collections.gob"
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	// write Gob encoded data into the file
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(c)
	if err != nil {
		return err
	}

	return nil
}

func (c *collectionStore) load() error {
	c.Lock()
	defer c.Unlock()

	// get the path using the globally available client (client) variable
	path := (&client).getDocumentRoot()
	path = path + string(os.PathSeparator) + "db_meta"

	// open the file where to save
	path = path + string(os.PathSeparator) + "registered_collections.gob"
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	// write Gob encoded data into the file
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(c)
	if err != nil {
		return err
	}

	return nil
}
