// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
		isInitialized bool // IsInitialized ensures that we don't initialize the client more than once, since doing that could lead to issues
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

	return p

}

// client is the instance of the Client struct
var cl Client

// InitClient setsup the package for use by an appliction. This should be called before the client can be used.
func Initialize(p ClientParams) error {
	// Although rare, it is still possible that two almost simultaneous calls are made to the Initialize function,
	// which could end up initializing the client twice and might overwrite the param values. Hence, we use a lock
	// to avoid that situation.
	(&cl).Lock()
	defer (&cl).Unlock()

	if cl.isInitialized {
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
	cl.ClientParams = p
	cl.isInitialized = true

	return nil
}

// GetClient returns the current instance of the client for the application. It panics if the client has not been initialized.
func GetClient() *Client {
	if !(&cl).isInitialized {
		log.Fatal("GoFiledb client fetched called without initializing the client")
	}
	return &cl
}

/********************************************************************************
* W R I T E
*********************************************************************************/

func (c *Client) Set(collectionName string, key string, data []byte) error {

	var dirPath string = c.getDirPath(collectionName, key)

	err := createDirIfNotExist(dirPath)
	if err != nil {
		return fmt.Errorf("error while creating the dir at path %s: %s", dirPath, err)
	}

	filepath := c.getFilePath(collectionName, key)

	err = ioutil.WriteFile(filepath, data, FILE_PERM)
	if err != nil {
		return fmt.Errorf("error while writing file: %s", err)
	}

	return nil
}

func (c *Client) SetStruct(collectionName string, key string, v interface{}) error {
	// Maybe we should allow nil
	// if v == nil {
	// 	return fmt.Errorf("[GoFiledb] Cannot save a nil file", key))
	// }

	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Set(collectionName, key, b)
}

func (c *Client) SetFromReader(collectionName, key string, src io.Reader) error {

	// ensure the directory exists
	var dirPath string = c.getDirPath(collectionName, key)
	err := createDirIfNotExist(dirPath)
	if err != nil {
		return fmt.Errorf("error while creating the dir at path %s: %s", dirPath, err)
	}

	filepath := c.getFilePath(collectionName, key)

	// open the file (copied from https://golang.org/src/io/ioutil/ioutil.go?s=2534:2602#L69)
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, src) // first argument is the number of bytes written
	if err != nil {
		return err
	}

	return nil

}

/********************************************************************************
* R E A D 																		*
*********************************************************************************/

func (c *Client) getFile(collectionName, key string) (*os.File, error) {

	filepath := c.getFilePath(collectionName, key)
	return os.Open(filepath)
}

// func (c *Client) getFileIfExist(collectionName, key string) (*os.File, error) {

// 	file, err := getFile(collectionName, key)
// 	if os.IsNotExists(err) {
// 		return nil, nil
// 	}
// 	if err != nil {
// 		return nil, err
// 	}
// 	return file, nil
// }

func (c *Client) Get(collectionName string, key string) ([]byte, error) {

	file, err := c.getFile(collectionName, key)
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

func (c *Client) GetIfExist(collectionName string, key string) ([]byte, error) {

	file, err := c.getFile(collectionName, key)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := bytes.NewBuffer(nil)

	_, err = io.Copy(buf, file) // the first discarded returnable is the number of bytes copied
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), err

}

func (c *Client) GetStruct(collectionName string, key string, v interface{}) error {

	bytes, err := c.Get(collectionName, key)
	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, v)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetStructIfExists(collectionName string, key string, v interface{}) (bool, error) {

	bytes, err := c.Get(collectionName, key)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return true, err
	}

	err = json.Unmarshal(bytes, v)
	if err != nil {
		return true, err
	}

	return true, nil
}

func (c *Client) GetIntoWriter(collectionName, key string, dest io.Writer) error {

	file, err := c.getFile(collectionName, key)
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
* H E L P E R 																	*
*********************************************************************************/

func (c *Client) getFilePath(collectionName, key string) string {
	return c.getDirPath(collectionName, key) + string(os.PathSeparator) + key
}

func (c *Client) getDirPath(collectionName, key string) string {
	dirs := []string{c.documentRoot, DATA_DIR_NAME, collectionName, c.getPartitionDirName(key)}
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
	h := c.getPartitionHash(key)
	return DATA_PARTITION_PREFIX + h
}

/* This function takes a string, convert each byte to a number representation and adds it, then returns a mod */
func (c *Client) getPartitionHash(str string) string {
	var sum int
	for i := 0; i < len(str); i++ {
		sum += int(str[i])
	}
	return strconv.Itoa(sum % c.numPartitions)
}
