package gofiledb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

// DocumentRoot is the absolute path to the directory that can be used for storing the files/data.
// Make sure that the user that runs the program has read/write access to DocumentRoot
type Client struct {
	DocumentRoot string
}

func GetClient(documentRoot string) *Client {
	var client Client = Client{
		DocumentRoot: documentRoot,
	}

	return &client
}

func (client *Client) SetDocumentRoot(documentRoot string) error {
	if client == nil {
		return fmt.Errorf("[GoFiledb] Panic: Tried to set DocumentRoot of a nil client.")
	}
	client.DocumentRoot = documentRoot
	return nil
}

/********************************************************************************
* W R I T E 																		*
*********************************************************************************/

func (client *Client) SetStruct(key string, v interface{}, path string) error {
	if v == nil {
		return errors.New(fmt.Sprintf("[nxjfsdb] The value provided by key %s is nil. Cannot store.", key))
	}

	valueJson, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return client.Set(key, valueJson, path)
}

func (client *Client) Set(key string, data []byte, path string) error {

	var folderPath string = client.getFullFolderPath(key, path)

	err := createDirIfNotExist(folderPath)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(folderPath+key, data, 0666)
	if err != nil {
		return err
	}

	return nil
}

/********************************************************************************
* R E A D 																		*
*********************************************************************************/

func (client *Client) Get(key string, path string) ([]byte, error) {

	var fullPath string = client.getFullFolderPath(key, path) + key

	buf := bytes.NewBuffer(nil)

	file, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(buf, file) // the first discarded returnable is the number of bytes copied
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), err

}

func (client *Client) GetStruct(key string, v interface{}, path string) error {

	bytes, err := client.Get(key, path)
	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, v)
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) GetStructIfExists(key string, v interface{}, path string) (bool, error) {

	bytes, err := client.Get(key, path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	err = json.Unmarshal(bytes, v)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (client *Client) GetFile(key string, path string) ([]byte, error) {
	var fullPath string = client.getFullFolderPath(key, path) + key

	file, err := ioutil.ReadFile(fullPath)
	return file, err
}

func (client *Client) GetFileIfExists(key string, path string) ([]byte, error) {
	file, err := client.GetFile(key, path)
	if IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return file, nil
}

/********************************************************************************
* H E L P E R 																	*
*********************************************************************************/
func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

func (client *Client) getFullFolderPath(key, path string) string {
	return client.DocumentRoot + path + getHashedFolderPath(key)
}

func createDirIfNotExist(path string) error {
	if _, _err := os.Stat(path); os.IsNotExist(_err) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			return nil
		}
	}
	return nil
}

/********************************************************************************
* H A S H I N G 																*
*********************************************************************************/

func getHashedFolderPath(key string) string {
	h := getHash(key)
	return "partition_" + h + "/"
}

/* This function takes a string, convert each byte to a number representation and adds it, then returns a mod */
func getHash(str string) string {
	var mod int = 50
	var sum int
	for i := 0; i < len(str); i++ {
		sum = int(str[i])
	}
	return strconv.Itoa(sum % mod)
}
