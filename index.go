package gofiledb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/teejays/clog"
	"io"
	"os"
	"reflect"
)

type (
	Index struct {
		FieldLocater string
		FieldType    string
		KeyValue     map[string][]string
	}
)

func (c *Client) AddIndex(collectionName string, fieldLocator string) error {
	cl, err := c.getCollectionByName(collectionName)
	if err != nil {
		return err
	}

	return cl.addIndex(fieldLocator)
}

// fieldName could be fieldA.fieldB, Components.Basic.Data.OrgId
func (cl Collection) addIndex(fieldLocater string) error {
	// go through all the docs in the collection and create a map...
	var index Index
	index.FieldLocater = fieldLocater
	index.KeyValue = make(map[string][]string)

	// get path for where all the collection data is
	collDataPath := joinPath(cl.DirPath, DATA_DIR_NAME)

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

		pDirPath := joinPath(collDataPath, pDirName)
		fileInfo, err := os.Stat(pDirPath)
		if err != nil {
			return err
		}
		if !fileInfo.IsDir() {
			clog.Warnf("%s: not a directory", pDirPath)
			continue
		}

		pDir, err := os.Open(pDirPath)
		if err != nil {
			return err
		}
		defer pDir.Close()

		docNames, err := pDir.Readdirnames(-1)
		if err != nil {
			return err
		}

		// open each of the doc, and add it to index
		for _, docName := range docNames {

			docPath := joinPath(pDirPath, docName)

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

			if cl.EncodingType != ENCODING_JSON {
				return fmt.Errorf("Indexing only supported for JSON data")
			}

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
					v_i := v.Interface()
					v_str := fmt.Sprintf("%v", v_i)

					index.FieldType = reflect.TypeOf(v_i).Kind().String()
					index.KeyValue[v_str] = append(index.KeyValue[v_str], docName)
				}
			}

		}

	}

	// Save the index file.. but first json encode it
	indexJson, err := json.Marshal(index)
	if err != nil {
		return err
	}

	indexPath := joinPath(cl.DirPath, META_DIR_NAME, fieldLocater)
	indexFile, err := os.Create(indexPath)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	_, err = indexFile.Write(indexJson)
	if err != nil {
		return err
	}

	return nil

}
