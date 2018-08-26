package gofiledb

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/teejays/clog"
	"io"
	//"log"
	"os"
	"reflect"
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

var ErrCollectionDoesNotExist = fmt.Errorf("Collection not found")

func (c *collectionStore) save() error {
	c.Lock()
	defer c.Unlock()

	// get the path using the globally available cl (client) variable
	path := (&cl).getDocumentRoot()
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
	path := (&cl).getDocumentRoot()
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
	DirPath string
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
	p = p.sanitize()

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

	// calculate the dir path for this collection
	coll.DirPath = c.getDirPathForCollection(p.Name)

	// create the dirs for the collection
	err := createDirIfNotExist(coll.DirPath + string(os.PathSeparator) + "meta")
	if err != nil {
		return err
	}
	err = createDirIfNotExist(coll.DirPath + string(os.PathSeparator) + "data")
	if err != nil {
		return err
	}
	cl.RegisteredCollections.Store[p.Name] = coll

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

	// Initialize the collection store if not initialized
	if cl.RegisteredCollections.Store == nil {
		return ErrCollectionDoesNotExist
	}

	// Don't repeat collection names
	if _, hasKey := cl.RegisteredCollections.Store[collectionName]; !hasKey {
		return ErrCollectionDoesNotExist
	}

	// Delete all the data & meta dirs for that collection
	dirPath := cl.getDirPathForCollection(collectionName)
	clog.Infof("Deleting data at %s...", dirPath)
	err := os.RemoveAll(dirPath)
	if err != nil {
		return err
	}

	// remove the reference in the registration store
	clog.Infof("Removing collection registration...")
	delete(cl.RegisteredCollections.Store, collectionName)

	return nil
}

// fieldName could be fieldA.fieldB, Components.Basic.Data.OrgId
func (coll Collection) AddIndex(fieldLocater string) error {
	// go through all the docs in the collection and create a map...
	var indexData map[string][]string = make(map[string][]string)

	// get path for where all the collection data is
	collDataPath := coll.DirPath + string(os.PathSeparator) + DATA_DIR_NAME

	// open the data dir, which has all the partition dirs
	collectionDataDir, err := os.Open(collDataPath)
	if err != nil {
		return err
	}
	defer collectionDataDir.Close()

	// get all the names of the partition dirs so we can open them
	partitionDirNames, err := collectionDataDir.Readdirnames(-1)
	if err != nil {
		return err
	}

	// for each partition dir, open it, make sures it's a Dir, and get all the files within it.
	for _, pDirName := range partitionDirNames {

		pDirPath := collDataPath + string(os.PathSeparator) + pDirName
		fileInfo, err := os.Stat(pDirPath)
		if err != nil {
			return err
		}
		if !fileInfo.IsDir() {
			clog.Warnf("%s is not a directory", pDirPath)
			continue
		}

		pDir, err := os.Open(pDirPath)
		if err != nil {
			return err
		}
		defer pDir.Close()

		docNames, err := pDir.Readdirnames(0)
		if err != nil {
			return err
		}

		fmt.Println(pDirPath)

		// open each of the doc, and add it to index
		for _, docName := range docNames {

			docPath := pDirPath + string(os.PathSeparator) + docName
			fmt.Println(docPath)

			docFile, err := os.Open(docPath)
			if err != nil {
				return nil
			}
			defer docFile.Close()

			// read the file into a json?
			buff := bytes.NewBuffer(nil)
			_, err = io.Copy(buff, docFile)
			if err != nil {
				return err
			}

			doc := buff.Bytes() // this is the json doc

			var docMap map[string]interface{}

			err = json.Unmarshal(doc, &docMap)
			if err != nil {
				return err
			}

			// get the value
			docMap_v := reflect.ValueOf(docMap)
			values, err := GetNestedFieldValues(docMap_v, fieldLocater)
			if err != nil {
				return err
			}

			// each of the 'values' correspond to the value for this doc for the given field
			// we shoud store them in the index
			for _, v := range values {
				// Todo: make sure that the values are hashable (i.e. string, int, float etc. and not map, channels etc.)
				if v.CanInterface() {
					v_str := fmt.Sprintf("%v", v.Interface())

					fmt.Println(v_str)

					indexData[v_str] = append(indexData[v_str], docName)
				}
			}

		}

	}

	// Save the index file.. but first json encode it
	indexDataJson, err := json.Marshal(indexData)
	if err != nil {
		return err
	}

	collMetaPath := coll.DirPath + string(os.PathSeparator) + META_DIR_NAME
	indexFile, err := os.Create(collMetaPath + string(os.PathSeparator) + fieldLocater)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	_, err = indexFile.Write(indexDataJson)
	if err != nil {
		return err
	}

	return nil

}
