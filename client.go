package gofiledb

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/teejays/clog"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

/********************************************************************************
* C L I E N T
*********************************************************************************/

// Client is the primary object that the external application interacts with while saving or fetching data
type Client struct {
	ClientParams
	collections   *collectionStore
	isInitialized bool // IsInitialized ensures that we don't initialize the client more than once, since doing that could lead to issues
}

type ClientParams struct {
	documentRoot string // documentRoot is the absolute path to the directory that can be used for storing the files/data
}

type clientParamsGob struct {
	DocumentRoot string
}

type ClientInitOptions struct {
	ClientParams
	overwritePreviousData bool // if true, gofiledb will remove all the existing data in the document root
}

func (p ClientParams) GobEncode() ([]byte, error) {
	var pGob clientParamsGob = clientParamsGob{
		DocumentRoot: p.documentRoot,
	}
	buff := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buff)
	err := enc.Encode(pGob)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (p *ClientParams) GobDecode(b []byte) error {

	buff := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buff)
	var pGob clientParamsGob
	err := dec.Decode(&pGob)
	if err != nil {
		return err
	}
	p.documentRoot = pGob.DocumentRoot
	return nil
}

const DEFAULT_CLIENT_NUM_PARTITIONS int = 2
const REGISTER_COLLECTION_FILE_NAME = "registered_collections.gob"

// client is the instance of the Client struct
var globalClient Client
var globalClientLock sync.RWMutex

var ErrClientAlreadyInitialized error = fmt.Errorf("Attempted to initialie GoFileDb client more than once")
var ErrClientNotInitialized error = fmt.Errorf("GoFiledb client fetched called without initializing the client")

/*** Initializers ***/

// Initialize setsup the package for use by an appliction. This should be called before the client can be used.
func Initialize(p ClientInitOptions) error {
	// Although rare, it is still possible that two almost simultaneous calls are made to the Initialize function,
	// which could end up initializing the client twice and might overwrite the param values. Hence, we use a lock
	// to avoid that situation.
	globalClientLock.Lock()
	defer globalClientLock.Unlock()

	if globalClient.isInitialized {
		return ErrClientAlreadyInitialized
	}

	// Ensure that the params provided make sense
	err := p.ClientParams.validate()
	if err != nil {
		return err
	}

	// Sanitize the params so they'r emore standard
	p.ClientParams = p.ClientParams.sanitize()

	var client Client
	client.ClientParams = p.ClientParams

	// If overwrite previousdata flag is passed, we shoudl delete existing data at document root
	if p.overwritePreviousData {
		err = client.Destroy()
		if err != nil {
			return err
		}
	}

	// Create the neccesary folders
	err = createDirIfNotExist(p.documentRoot)
	if err != nil {
		return err
	}
	err = createDirIfNotExist(joinPath(p.documentRoot, DATA_DIR_NAME))
	if err != nil {
		return err
	}
	err = createDirIfNotExist(joinPath(p.documentRoot, META_DIR_NAME))
	if err != nil {
		return err
	}

	// Check if we already have a client that is intitilzed at this Document Root
	err = client.getGlobalMetaStruct("globalClient.gob", &client)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// By this point, either the existing client has been loaded to client var, or not.
	// If client.isInitialized == true, then the existing client has been loaded.
	if client.isInitialized {
		clog.Warnf("Existing GoFileDb client found at %s. Loading it.", p.documentRoot)
		// Ensure that the loaded params match the new params provided
		// For now, the only param that matters is document root.
		if client.documentRoot != p.documentRoot {
			return fmt.Errorf("An existing GoFileDb client has been found at the location %s. However, that client's documentRoot is set to %s. This is an unexpected error.", p.documentRoot, client.documentRoot)
		}
		if client.collections == nil {
			return fmt.Errorf("An existing GoFileDb client has been found at the location %s. However, that client does not have an initialized collection data. This is an unexpected error.", p.documentRoot)
		}

		return nil
	}

	// Code here corresponds to the case when we're creating a new Client
	// Initialize the CollectionStore
	collections := new(collectionStore)             // collections is a pointer to collectionStore
	collections.Store = make(map[string]Collection) // default case

	// Initialize the collection store
	// err = client.getGlobalMetaStruct("registered_collections.gob", &collections.Store)
	// if err != nil && !os.IsNotExist(err) {
	// 	return err
	// }

	client.collections = collections

	client.isInitialized = true

	globalClient = client

	return SaveGlobalClientToDisk()
}

func SaveGlobalClientToDisk() error {
	return (&globalClient).setGlobalMetaStruct("globalClient.gob", globalClient)
}

// GetClient returns the current instance of the client for the application. It panics if the client has not been initialized.
func GetClient() *Client {
	if !(&globalClient).isInitialized {
		log.Panic("GoFiledb client fetched called without initializing the client")
	}
	return &globalClient
}

/*** Local Getters & Setters ***/

func (c *Client) getDocumentRoot() string {
	return c.documentRoot
}
func (c *Client) getIsInitialized() bool {
	return c.isInitialized
}
func (c *Client) getCollections() *collectionStore {
	return c.collections
}

// func (c *Client) setCollections(cl *collectionStore) {
// 	c.collections = cl
// }
func (c *Client) getCollectionByName(collectionName string) (*Collection, error) {
	c.collections.RLock()
	defer c.collections.RUnlock()

	cl, hasKey := c.collections.Store[collectionName]
	if !hasKey {
		return nil, ErrCollectionIsNotExist
	}
	return &cl, nil
}

func (c *Client) Destroy() error {
	// remove everything related to this client, and refresh it
	err := os.RemoveAll(c.getDocumentRoot())
	if err != nil {
		return err
	}
	c = &Client{}
	return nil
}
func (c *Client) FlushAll() error {
	return os.RemoveAll(c.documentRoot)
}

func (c *Client) setGlobalMetaStruct(metaName string, v interface{}) error {
	file, err := os.Create(joinPath(c.getDocumentRoot(), META_DIR_NAME, metaName))
	if err != nil {
		return err
	}

	enc := gob.NewEncoder(file)
	err = enc.Encode(v)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) getGlobalMetaStruct(metaName string, v interface{}) error {
	file, err := os.Open(joinPath(c.getDocumentRoot(), META_DIR_NAME, metaName))
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(file)
	err = dec.Decode(v)
	if err != nil {
		return err
	}
	return nil
}

/********************************************************************************
* C L I E N T  <->  C O L L E C T I O N
*********************************************************************************/

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
	c.collections.RLock()
	_, hasKey := c.collections.Store[p.Name]
	c.collections.RUnlock()
	if hasKey {
		return fmt.Errorf("A collection with name %s already exists", p.Name)
	}

	// Create the required dir paths for this collection
	cl.DirPath = c.getDirPathForCollection(p.Name)

	// create the dirs for the collection
	err = createDirIfNotExist(joinPath(cl.DirPath, DATA_DIR_NAME))
	if err != nil {
		return err
	}
	err = createDirIfNotExist(joinPath(cl.DirPath, META_DIR_NAME))
	if err != nil {
		return err
	}

	err = createDirIfNotExist(cl.getIndexDirPath())
	if err != nil {
		return err
	}

	// Initialize the IndexStore, which stores info on the indexes associated with this Collection
	cl.IndexStore.Store = make(map[string]IndexInfo)

	// Register the Collection

	c.collections.Lock()
	defer c.collections.Unlock()

	// Initialize the collection store if not initialized (but it should already be initialized because of the Initialize() function)
	if c.collections.Store == nil {
		c.collections.Store = make(map[string]Collection)
	}
	c.collections.Store[p.Name] = cl

	// Save the data so it persists (might deprecate as we start storing the entire client)
	err = c.setGlobalMetaStruct(REGISTER_COLLECTION_FILE_NAME, c.collections.Store)
	if err != nil {
		return err
	}

	// Save the client to disk
	err = SaveGlobalClientToDisk()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) RemoveCollection(collectionName string) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	// Unregister the collection from the Client's Collection Store
	c.collections.Lock()
	defer c.collections.Unlock()
	clog.Infof("Removing collection registration...")
	delete(c.collections.Store, collectionName)

	// Save the data so it persists (might deprecate as we start storing the entire client)
	err = c.setGlobalMetaStruct("registered_collections.gob", c.collections.Store)
	if err != nil {
		return err
	}

	// Delete all the data & meta dirs for that collection
	clog.Infof("Deleting data at %s...", cl.DirPath)
	err = os.RemoveAll(cl.DirPath)
	if err != nil {
		return err
	}

	// Save the client to disk
	err = SaveGlobalClientToDisk()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) IsCollectionExist(collectionName string) (bool, error) {
	collectionName = strings.TrimSpace(collectionName)
	collectionName = strings.ToLower(collectionName)

	_, err := c.getCollectionByName(collectionName)

	if err == ErrCollectionIsNotExist {
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

func (c *Client) Set(collectionName string, k Key, data []byte) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.set(k, data)
}

func (c *Client) SetStruct(collectionName string, k Key, v interface{}) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.setFromStruct(k, v)
}

func (c *Client) SetFromReader(collectionName string, k Key, src io.Reader) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.setFromReader(k, src)
}

/********************************************************************************
* R E A D E R S
*********************************************************************************/

func (c *Client) GetFile(collectionName string, k Key) (*os.File, error) {
	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return nil, err
	}

	return cl.getFile(k)
}

func (c *Client) Get(collectionName string, k Key) ([]byte, error) {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return nil, err
	}

	return cl.getFileData(k)
}

func (c *Client) GetIfExist(collectionName string, k Key) ([]byte, error) {

	data, err := c.Get(collectionName, k)
	if os.IsNotExist(err) { // if doesn't exist, return nil
		return nil, nil
	}
	return data, err
}

func (c *Client) GetStruct(collectionName string, k Key, dest interface{}) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.getIntoStruct(k, dest)
}

func (c *Client) GetStructIfExists(collectionName string, k Key, dest interface{}) (bool, error) {

	err := c.GetStruct(collectionName, k, dest)
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (c *Client) GetIntoWriter(collectionName string, k Key, dest io.Writer) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}
	return cl.getIntoWriter(k, dest)
}

/********************************************************************************
* Q U E R Y (B E T A)
*********************************************************************************/

func (c *Client) Search(collectionName string, query string) (SearchResponse, error) {

	start := time.Now()
	var resp SearchResponse = SearchResponse{}

	defer func() {
		resp.TimeTaken = time.Now().Sub(start)
	}()

	resp.Query = query
	resp.Collection = collectionName

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		resp.Error = err
		return resp, err
	}

	resp.Result, err = cl.search(query)
	if err != nil {
		resp.Error = err
		return resp, err
	}

	resp.NumDocuments = len(resp.Result)

	return resp, nil

}

func (c *Client) AddIndex(collectionName string, fieldLocator string) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	err = cl.addIndex(fieldLocator)
	if err != nil {
		return err
	}

	// Save the client to disk
	return SaveGlobalClientToDisk()

}

/********************************************************************************
* N A V I G A T I O N   H E L P E R S
*********************************************************************************/

func (c *Client) getDirPathForCollection(collectionName string) string {
	dirs := []string{c.documentRoot, DATA_DIR_NAME, collectionName}
	return strings.Join(dirs, string(os.PathSeparator))
}

/********************************************************************************
* C L I E N T  P A R A M S
*********************************************************************************/

func NewClientParams(documentRoot string) ClientParams {
	var params ClientParams = ClientParams{
		documentRoot: documentRoot,
	}
	return params
}

func (p ClientParams) validate() error {
	// documentRoot shall not be totally white
	if strings.TrimSpace(p.documentRoot) == "" {
		return fmt.Errorf("Empty documentRoot field provided")
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
