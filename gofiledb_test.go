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

var userCollectionName string = "users"

var userCollectionProps CollectionProps = CollectionProps{
	Name:                  "Users",
	EncodingType:          ENCODING_JSON,
	EnableGzipCompression: false,
	NumPartitions:         1,
}

var mock_user_1_key string = "mock_user_1_key"
var mock_user_1_data User = User{
	UserId:  1234,
	Name:    "John Doe",
	Address: "123 Main Street, ME 12345",
	Age:     25,
	Org:     Org{1},
}

var mock_user_2_key string = "mock_user_2_key"
var mock_user_2_data User = User{
	UserId:  2,
	Name:    "Jane Does",
	Address: "123 Main Street, ME 12345",
	Age:     25,
	Org:     Org{261},
}

/********************************************************************************
* T E S T S 																	*
*********************************************************************************/

func TestInitClient(t *testing.T) {
	usr, err := user.Current()
	if err != nil {
		log.Fatalf("[TestInitClient] %v", err)
	}
	var home string = usr.HomeDir
	var document_root string = home + "/" + "gofiledb_test"
	params := ClientParams{
		documentRoot:       document_root,
		numPartitions:      50,
		ignorePreviousData: true,
	}
	err = Initialize(params)
	if err != nil {
		log.Fatalf("[TestInitClient] %v", err)
	}

	_ = GetClient()

}

func TestGetClient(t *testing.T) {
	GetClient()
}

func TestAddCollection(t *testing.T) {
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

func TestSetSructOverwrite(t *testing.T) {
	key := mock_user_2_key
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

func assertUserDataByKey(key string, expectedData interface{}) error {
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

func TestGetStruct(t *testing.T) {

	key := mock_user_2_key
	data := mock_user_1_data

	err := assertUserDataByKey(key, data)
	if err != nil {
		t.Error(err)
	}
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
