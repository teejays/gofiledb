// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	DATA_DIR_NAME string = "data"
	META_DIR_NAME string = "meta"

	DATA_PARTITION_PREFIX string = "partition_"

	FILE_PERM = 0660
	DIR_PERM  = 0750
)

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
	registeredCollections := new(collectionStore) // registeredCollections is a pointer to collectionStore
	registeredCollections.Lock()
	defer registeredCollections.Unlock()

	// Create the neccesary folders
	err = createDirIfNotExist(p.getDocumentRoot())
	if err != nil {
		return err
	}
	err = createDirIfNotExist(joinPath(p.getDocumentRoot(), DATA_DIR_NAME))
	if err != nil {
		return err
	}
	err = createDirIfNotExist(joinPath(p.getDocumentRoot(), META_DIR_NAME))
	if err != nil {
		return err
	}

	registeredCollections.Store = make(map[string]Collection) // default case
	err = client.getGlobalMetaStruct("registered_collections.gob", &registeredCollections.Store)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	client.setRegisteredCollections(registeredCollections)

	client.isInitialized = true

	return nil
}

/********************************************************************************
* H E L P E R 																	*
*********************************************************************************/

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

/********************************************************************************
* Collection Store
*********************************************************************************/

// func (c *collectionStore) save() error {
// 	c.Lock()
// 	defer c.Unlock()

// 	// get the path using the globally available client (client) variable
// 	path := (&client).getDocumentRoot()
// 	path = path + string(os.PathSeparator) + "db_meta"
// 	err := createDirIfNotExist(path)
// 	if err != nil {
// 		return err
// 	}

// 	// open the file where to save
// 	path = path + string(os.PathSeparator) + "registered_collections.gob"
// 	file, err := os.Create(path)
// 	if err != nil {
// 		return err
// 	}

// 	// write Gob encoded data into the file
// 	encoder := gob.NewEncoder(file)
// 	err = encoder.Encode(c)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func loadRegisteredCollectionsMeta() (collectionStore error {
// 	c.Lock()
// 	defer c.Unlock()

// 	// get the path using the globally available client (client) variable
// 	path := (&client).getDocumentRoot()
// 	path = path + string(os.PathSeparator) + "db_meta"

// 	// open the file where to save
// 	path = path + string(os.PathSeparator) + "registered_collections.gob"
// 	file, err := os.Open(path)
// 	if err != nil {
// 		return err
// 	}

// 	// write Gob encoded data into the file
// 	decoder := gob.NewDecoder(file)
// 	err = decoder.Decode(c)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }
