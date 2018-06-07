// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

// Client is the primary object that the application interacts with while saving or fetching data
type Client struct {
	// DocumentRoot is the absolute path to the directory that can be used for storing the files/data
	DocumentRoot string
}

// client is the instance of the Client struct
var client Client

// InitClient setsup the package for use by an appliction. This should be called before the client can be used.
func InitClient(documentRoot string) {
	client = Client{
		DocumentRoot: documentRoot,
	}
}

// GetClient returns the current instance of the client for the application. It panics if the client has not been initialized.
func GetClient() *Client {
	if client.DocumentRoot == "" {
		log.Fatal("[GoFiledb] GetClient called without initializing the client")
	}
	return &client
}

/********************************************************************************
* W R I T E 																		*
*********************************************************************************/

func (c *Client) Set(path string, key string, data []byte) error {

	var folderPath string = c.fullDirPath(path, key)

	err := createDirIfNotExist(folderPath)
	if err != nil {
		return fmt.Errorf("error while creating the folder path: %v", err)
	}

	err = ioutil.WriteFile(c.fullFilePath(path, key), data, 0666)
	if err != nil {
		return fmt.Errorf("error while writing file: %v", err)
	}

	return nil
}

func (c *Client) SetStruct(path string, key string, v interface{}) error {
	if v == nil {
		return errors.New(fmt.Sprintf("[GoFiledb] The value provided by key %s is nil. Cannot store.", key))
	}

	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Set(path, key, b)
}

/********************************************************************************
* R E A D 																		*
*********************************************************************************/

func (c *Client) Get(path string, key string) ([]byte, error) {

	buf := bytes.NewBuffer(nil)

	file, err := os.Open(c.fullFilePath(path, key))
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(buf, file) // the first discarded returnable is the number of bytes copied
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), err

}

func (c *Client) GetStruct(path string, key string, v interface{}) error {

	bytes, err := c.Get(path, key)
	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, v)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetStructIfExists(path string, key string, v interface{}) (bool, error) {

	bytes, err := c.Get(path, key)
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

func (c *Client) GetFile(path string, key string) ([]byte, error) {
	var fullPath string = c.fullFilePath(path, key)

	file, err := ioutil.ReadFile(fullPath)
	return file, err
}

func (c *Client) GetFileIfExists(path string, key string) ([]byte, error) {
	file, err := c.GetFile(path, key)
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

func (c *Client) fullFilePath(path, key string) string {
	return c.fullDirPath(path, key) + "/" + key
}

func (c *Client) fullDirPath(path, key string) string {
	return c.DocumentRoot + "/" + path + "/" + hashedFolderName(key)
}

func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

func createDirIfNotExist(path string) error {
	if _, _err := os.Stat(path); os.IsNotExist(_err) {
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil
		}
	}
	return nil
}

func (c *Client) FlushAll() error {
	return os.RemoveAll(c.DocumentRoot)
}

/********************************************************************************
* H A S H I N G 																*
*********************************************************************************/
// This section is used to spread files across multiple directories (so one folder doesn't end up with too many files).
var hashModConstant int = 50

func hashedFolderName(key string) string {
	h := getHash(key)
	return "partition_" + h
}

/* This function takes a string, convert each byte to a number representation and adds it, then returns a mod */
func getHash(str string) string {
	var sum int
	for i := 0; i < len(str); i++ {
		sum = int(str[i])
	}
	return strconv.Itoa(sum % hashModConstant)
}
