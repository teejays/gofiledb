package gofiledb

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/teejays/clog"
	"github.com/teejays/gofiledb/collection"
	"github.com/teejays/gofiledb/key"
	"github.com/teejays/gofiledb/util"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const DEFAULT_CLIENT_NUM_PARTITIONS int = 2

// globalClient is the instance of the Client struct
var globalClient Client
var globalClientLock sync.RWMutex

// Errors
var ErrClientAlreadyInitialized error = fmt.Errorf("Attempted to initialie GoFileDb client more than once")
var ErrClientNotInitialized error = fmt.Errorf("GoFiledb client fetched called without initializing the client")

/********************************************************************************
* C L I E N T
*********************************************************************************/

// Client is the primary object that the external application interacts with while saving or fetching data
type Client struct {
	isInitialized bool // IsInitialized ensures that we don't initialize the client more than once, since doing that could lead to issues
	collections   *collectionStore
	ClientParams
}

type collectionStore struct {
	Store map[string]collection.Collection
	sync.RWMutex
}

type ClientParams struct {
	documentRoot string // documentRoot is the absolute path to the directory that can be used for storing the files/data
}

type clientParamsGob struct {
	DocumentRoot string
}

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

func (c *Client) getCollectionByName(_collectionName string) (*collection.Collection, error) {
	c.collections.RLock()
	defer c.collections.RUnlock()

	collectionName := strings.ToLower(_collectionName)
	cl, hasKey := c.collections.Store[collectionName]
	if !hasKey {
		return nil, collection.ErrCollectionIsNotExist
	}
	return &cl, nil
}

func (c *Client) Destroy() error {
	// remove everything related to this client, and refresh it
	clog.Debugf("Destroying all the data at: %s", c.documentRoot)
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

func (c *Client) save() error {
	return c.setMeta("globalClient.gob", globalClient)
}

func (c *Client) setMeta(metaName string, v interface{}) error {
	clog.Debugf("Setting globalMetaStruct: %s", metaName)
	file, err := os.Create(util.JoinPath(c.getDocumentRoot(), util.META_DIR_NAME, metaName))
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

func (c *Client) getMeta(metaName string, v interface{}) error {
	clog.Debugf("Getting globalMetaStruct: %s", metaName)
	file, err := os.Open(util.JoinPath(c.getDocumentRoot(), util.META_DIR_NAME, metaName))
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

func (c *Client) AddCollection(_p CollectionProps) error {

	p := collection.CollectionProps(_p)

	// Sanitize the collection props
	p = p.Sanitize()

	// Validate the collection props
	err := p.Validate()
	if err != nil {
		return err
	}

	// Create a Colelction and add to registered collections
	var cl collection.Collection
	cl.CollectionProps = p

	// Don't repeat collection names
	c.collections.RLock()
	_, hasKey := c.collections.Store[p.Name]
	c.collections.RUnlock()
	if hasKey {
		return collection.ErrCollectionIsExist
	}

	// Create the required dir paths for this collection
	cl.DirPath = c.getDirPathForCollection(p.Name)

	// create the dirs for the collection
	err = util.CreateDirIfNotExist(util.JoinPath(cl.DirPath, util.DATA_DIR_NAME))
	if err != nil {
		return err
	}
	err = util.CreateDirIfNotExist(util.JoinPath(cl.DirPath, util.META_DIR_NAME))
	if err != nil {
		return err
	}

	err = util.CreateDirIfNotExist(cl.GetDirPathForIndexes())
	if err != nil {
		return err
	}

	// Initialize the IndexStore, which stores info on the indexes associated with this Collection
	cl.IndexStore.Store = make(map[string]collection.IndexInfo)

	// Register the Collection

	c.collections.Lock()
	defer c.collections.Unlock()

	// Initialize the collection store if not initialized (but it should already be initialized because of the Initialize() function)
	if c.collections.Store == nil {
		c.collections.Store = make(map[string]collection.Collection)
	}
	c.collections.Store[p.Name] = cl

	// Save the client to disk
	err = c.save()
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

	// Delete all the data & meta dirs for that collection
	clog.Infof("Deleting data at %s...", cl.DirPath)
	err = os.RemoveAll(cl.DirPath)
	if err != nil {
		return err
	}

	// Save the client to disk
	err = c.save()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) IsCollectionExist(collectionName string) (bool, error) {
	collectionName = strings.TrimSpace(collectionName)
	collectionName = strings.ToLower(collectionName)

	_, err := c.getCollectionByName(collectionName)

	if err == collection.ErrCollectionIsNotExist {
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

func (c *Client) Set(collectionName string, k key.Key, data []byte) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.Set(k, data)
}

func (c *Client) SetStruct(collectionName string, k key.Key, v interface{}) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.SetFromStruct(k, v)
}

/********************************************************************************
* R E A D E R S
*********************************************************************************/

func (c *Client) GetFile(collectionName string, k key.Key) (*os.File, error) {
	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return nil, err
	}

	return cl.GetFile(k)
}

func (c *Client) Get(collectionName string, k key.Key) ([]byte, error) {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return nil, err
	}

	return cl.GetFileData(k)
}

func (c *Client) GetIfExist(collectionName string, k key.Key) ([]byte, error) {

	data, err := c.Get(collectionName, k)
	if os.IsNotExist(err) { // if doesn't exist, return nil
		return nil, nil
	}
	return data, err
}

func (c *Client) GetStruct(collectionName string, k key.Key, dest interface{}) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.GetIntoStruct(k, dest)
}

func (c *Client) GetStructIfExists(collectionName string, k key.Key, dest interface{}) (bool, error) {

	err := c.GetStruct(collectionName, k, dest)
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (c *Client) GetIntoWriter(collectionName string, k key.Key, dest io.Writer) error {

	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}
	return cl.GetIntoWriter(k, dest)
}

/********************************************************************************
* Q U E R Y (B E T A)
*********************************************************************************/

type SearchResponse struct {
	Collection   string
	Query        string
	Error        error
	TimeTaken    time.Duration
	NumDocuments int
	Result       []interface{}
}

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

	resp.Result, err = cl.Search(query)
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

	err = cl.AddIndex(fieldLocator)
	if err != nil {
		return err
	}

	// Save the client to disk
	return c.save()

}

/********************************************************************************
* N A V I G A T I O N   H E L P E R S
*********************************************************************************/

func (c *Client) getDirPathForCollection(collectionName string) string {
	dirs := []string{c.documentRoot, util.DATA_DIR_NAME, collectionName}
	return strings.Join(dirs, string(os.PathSeparator))
}
