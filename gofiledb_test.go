// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"fmt"
	"log"
	"os/user"
	"testing"
)

const REMOVE_COLLECTION = false
const DESTROY = false

/********************************************************************************
* M O C K  D A T A 																*
*********************************************************************************/

type User struct {
	UserId  int
	Name    string
	Address string
	Age     int
	Org     Org
}
type Org struct {
	OrgId int64
}

var mockInitOptions1 ClientInitOptions = ClientInitOptions{
	overwritePreviousData: true,
}
var mockInitOptions2 ClientInitOptions = ClientInitOptions{
	overwritePreviousData: false,
}

var userCollectionName string = "users"

var userCollectionProps CollectionProps = CollectionProps{
	Name:                  "Users",
	EncodingType:          ENCODING_JSON,
	EnableGzipCompression: false,
	NumPartitions:         1,
}

var mock_user_1_key Key = 1
var mock_user_1_data User = User{
	UserId:  1234,
	Name:    "John Doe",
	Address: "123 Main Street, ME 12345",
	Age:     25,
	Org:     Org{1},
}

var mock_user_1b_key Key = 1
var mock_user_1b_data User = User{
	UserId:  1234,
	Name:    "John Doe B",
	Address: "123 Main Street, ME 12345",
	Age:     30,
	Org:     Org{1},
}

var mock_user_2_key Key = 2
var mock_user_2_data User = User{
	UserId:  493,
	Name:    "Jane Does",
	Address: "123 Main Street, ME 12345",
	Age:     25,
	Org:     Org{261},
}

var mock_user_3_key Key = 3
var mock_user_3_data User = User{
	UserId:  973,
	Name:    "Joe Dies",
	Address: "123 Main Street, ME 12345",
	Age:     26,
	Org:     Org{1},
}

var documentRoot string

func init() {
	usr, err := user.Current()
	if err != nil {
		log.Fatalf("[Init] %v", err)
	}
	documentRoot = joinPath(usr.HomeDir, "gofiledb_test")
}

/********************************************************************************
* T E S T S 																	*
*********************************************************************************/

/*
 * Client Tests
 */

// TestGetClientPreInit: Makes sure we get a ClientNotInitialized Error when getting a client which has not been initialized
func TestGetClientPreInit(t *testing.T) {
	defer func() {
		// it should panic
		if r := recover(); r == nil {
			t.Error("Expected Panic with ErrClientNotInitialized error but got nil")
			return
		} else if r.(string) != ErrClientNotInitialized.Error() {
			t.Errorf("Expected Panic with ErrClientNotInitialized error but got: %s", r)
		}
	}()

	_ = GetClient()

}

// TestInitializeClient: Makes sure we can initialize a fresh copy of a client at documentRoot
func TestInitializeClient(t *testing.T) {

	mockInitOptions1.documentRoot = documentRoot

	err := Initialize(mockInitOptions1)
	if err != nil {
		log.Fatalf("[TestInitClient] %v", err)
	}

	_ = GetClient()

}

// TestInitializeClient: Makes sure we can initialize a fresh copy of a client at documentRoot
func TestInitializeClientTwo(t *testing.T) {

	mockInitOptions2.documentRoot = documentRoot

	err := Initialize(mockInitOptions2)
	if err != nil && err != ErrClientAlreadyInitialized {
		log.Fatalf("[TestInitClient] %v", err)
	}
	if err == nil {
		t.Error("Expected ErrClientAlreadyInitialized error but got nil")
	}

}

// TestGetClient: Makes sure we can get the initialized client
func TestGetClient(t *testing.T) {
	_ = GetClient()
}

/*
 * Collection Tests
 */

func TestIsCollectionExistFail(t *testing.T) {
	client := GetClient()
	exists, err := client.IsCollectionExist(userCollectionProps.Name)
	if err != nil {
		t.Error(err)
	}
	if exists {
		t.Error("Expected collection to not exist, but it exists")
	}
}

func TestAddCollectionOne(t *testing.T) {
	client := GetClient()

	err := client.AddCollection(userCollectionProps)
	if err != nil {
		t.Error(err)
	}
}

func TestIsCollectionExist(t *testing.T) {
	client := GetClient()
	exists, err := client.IsCollectionExist(userCollectionProps.Name)
	if err != nil {
		t.Error(err)
	}
	if !exists {
		t.Errorf("Expected collections %s to exist but received false for IsCollectionExist method", userCollectionProps.Name)
	}
}

/*
 * Index Tests
 */

func TestAddIndex(t *testing.T) {
	client := GetClient()
	err := client.AddIndex(userCollectionName, "Age")
	if err != nil {
		t.Error(err)
	}

	err = client.AddIndex(userCollectionName, "Org.OrgId")
	if err != nil {
		t.Error(err)
	}
}

/*
 * Data Write
 */

func TestSetStructFirst(t *testing.T) {
	key := mock_user_1_key
	data := mock_user_1_data

	client := GetClient()
	err := client.SetStruct(userCollectionName, key, data)
	if err != nil {
		t.Error(err)
	}
	err = assertUserDataByKey(key, data)
	if err != nil {
		t.Error(err)
	}
}

func TestSetStructSecond(t *testing.T) {
	key := mock_user_2_key
	data := mock_user_2_data

	client := GetClient()
	err := client.SetStruct(userCollectionName, key, data)
	if err != nil {
		t.Error(err)
	}
	err = assertUserDataByKey(key, data)
	if err != nil {
		t.Error(err)
	}
}

func TestSetStructThird(t *testing.T) {
	key := mock_user_3_key
	data := mock_user_3_data

	client := GetClient()
	err := client.SetStruct(userCollectionName, key, data)
	if err != nil {
		t.Error(err)
	}
	err = assertUserDataByKey(key, data)
	if err != nil {
		t.Error(err)
	}
}

func TestSetSructOverwrite(t *testing.T) {
	key := mock_user_1b_key
	data := mock_user_1b_data

	client := GetClient()
	err := client.SetStruct(userCollectionName, key, data)
	if err != nil {
		t.Error(err)
	}
	err = assertUserDataByKey(key, data)
	if err != nil {
		t.Error(err)
	}
}

func assertUserDataByKey(key Key, expectedData interface{}) error {
	client := GetClient()

	var data User
	err := client.GetStruct(userCollectionName, key, &data)
	if err != nil {
		return err
	}
	if data != expectedData {
		return fmt.Errorf("Fectched data did not match expected data: \n Fetched: %v \n Expected: %v", data, expectedData)
	}

	return nil
}

/*
 * Data Read
 */

func TestGetStruct(t *testing.T) {

	key := mock_user_1b_key
	data := mock_user_1b_data

	err := assertUserDataByKey(key, data)
	if err != nil {
		t.Error(err)
	}
}

/*
 * Data Search
 */

func TestSearch(t *testing.T) {
	c := GetClient()
	results, err := c.Search(userCollectionName, "Age:25")
	if err != nil {
		t.Error(err)
	}

	err = assertSearchResult(results, 1, []string{"Jane Does"})
	if err != nil {
		fmt.Println(results)
		t.Error(err)
	}

	results, err = c.Search(userCollectionName, "Org.OrgId:1")
	if err != nil {
		t.Error(err)
	}
	err = assertSearchResult(results, 2, []string{"Joe Dies", "John Doe B"})
	if err != nil {
		fmt.Println(results)
		t.Error(err)
	}

	results, err = c.Search(userCollectionName, "Org.OrgId:1+Age:26")
	if err != nil {
		t.Error(err)
	}
	err = assertSearchResult(results, 1, []string{"Joe Dies"})
	if err != nil {
		fmt.Println(results)
		t.Error(err)
	}

	results, err = c.Search(userCollectionName, "Org.OrgId:1+Age:26+Name:Tom")
	if err != nil && err != ErrIndexNotImplemented {
		t.Error(err)
	}
	if err != ErrIndexNotImplemented {
		t.Error(fmt.Errorf("Expected ErrIndexNotImplemented got: %v, %s", results, err))
	}

}

func assertSearchResult(resp SearchResponse, expectedLength int, names []string) error {
	if resp.NumDocuments != expectedLength {
		return fmt.Errorf("number of results returned %d do not match the expected number %d", resp.NumDocuments, expectedLength)
	}
	for _, n := range names {

		var exists bool
		for i, _r := range resp.Result {
			r, ok := _r.(map[string]interface{})
			if !ok {
				return fmt.Errorf("error asserting the row %d of results as a map[string]interface{}", i+1)
			}
			if n == r["Name"] {
				exists = true
			}

		}
		if !exists {
			return fmt.Errorf("Expected a document with name %s but did not find it in result.", n)
		}

	}

	return nil
}

func TestRemoveCollection(t *testing.T) {
	if !REMOVE_COLLECTION {
		log.Println("Leaving collection data as is")
		return
	}
	client := GetClient()
	err := client.RemoveCollection(userCollectionName)
	if err != nil {
		t.Error(err)
	}
}

func TestDestroy(t *testing.T) {
	if !DESTROY {
		log.Println("Not destorying the db")
		return
	}
	client := GetClient()
	err := client.Destroy()
	if err != nil {
		t.Error(err)
	}
}
