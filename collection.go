package gofiledb

import (
	"encoding/gob"
	"fmt"
	"strings"
	"sync"
)

/********************************************************************************
* C O L L E C T I O N
*********************************************************************************/
type collectionStore struct {
	Store map[string]Collection
	sync.RWMutex
}

func (c *collectionStore) save() error {
	c.Lock()
	defer c.Unlock()

	// get the path using the globally available cl (client) variable
	path := &(cl).getDocumentRoot()
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

	// get the path using the globally available cl (client) variable
	path := &(cl).getDocumentRoot()
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

type Collection struct {
	CollectionProps
}

type CollectionProps struct {
	Name string
}

func (p CollectionProps) sanitize() CollectionProps {
	p.Name = strings.TrimSpace(p.Name)
	p.Name = strings.ToLower(p.Name)
	return p
}

func (c *Client) AddCollection(p CollectionProps) error {
	cl.RegisteredCollections.Lock()
	defer cl.RegisteredCollections.Unlock()

	// Initialize the collection store if not initialized
	if cl.RegisteredCollections.Store == nil {
		cl.RegisteredCollections.Store = make(map[string]Collection)
	}

	// Sanitize the collection props
	p = sanitize()

	// Don't repeat collection names
	if _, hasKey := cl.RegisteredCollections.Store[p.Name]; hasKey {
		return fmt.Errorf("A collection with name %s already exists", p.Name)
	}

	// Validate the collection props
	if p.Name == "" {
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

	// Create a Colelction and add to registered collections
	var coll Collection
	coll.CollectionProps = p

	cl.RegisteredCollections[p.Name] = coll

	return nil
}

func (c *Client) RemoveCollection(collectionName string) error {
	cl.RegisteredCollections.Lock()
	defer cl.RegisteredCollections.Unlock()

	// sanitize
	collectionName = strings.TrimSpace(collectionName)
	// Validate the collection props
	if collectionName == "" {
		return fmt.Errorf("Invalid Collection name")
	}

	var ErrCollectionDoesNotExist = fmt.Errorf("Collection not found")
	// Initialize the collection store if not initialized
	if cl.RegisteredCollections.Store == nil {
		return ErrCollectionDoesNotExist
	}

	// Don't repeat collection names
	if _, hasKey := cl.RegisteredCollections.Store[collectionName]; !hasKey {
		return ErrCollectionDoesNotExist
	}

	// Delete all the data & meta data for that collection
	dirPath := cl.getDirPathForCollection(collectionName)
	clog.Infof("Deleting data at %d...", dirPath)
	err := os.RemoveAll(dirPath)
	if err != nil {
		return err
	}

	clog.Infof("Removing collection registration...")
	delete(cl.RegisteredCollections, collectionName)

	return nil
}

func (c Collection) AddIndex(name string)
