// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"log"
	"os/user"
	"testing"
)

/********************************************************************************
* M O C K  D A T A 																*
*********************************************************************************/

type user_struct struct {
	UserId  int
	Name    string
	Address string
	Age     int
}

var user_collection_name string = "users"

var mock_user_1_key string = "mock_user_1_key"
var mock_user_1_data user_struct = user_struct{
	UserId:  1234,
	Name:    "John Doe",
	Address: "123 Main Street, ME 12345",
	Age:     30,
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
	var document_root string = home + "/" + "gofiledb_test_data"
	InitClient(document_root)

	if GetClient().DocumentRoot != document_root {
		t.Errorf("DocumentRoot of the local client is not as expected. Expected %s but got %s.", document_root, GetClient().DocumentRoot)
	}
}

func TestGetClient(t *testing.T) {
	GetClient()
}

func TestSetStruct(t *testing.T) {
	client := GetClient()
	err := client.SetStruct(user_collection_name, mock_user_1_key, mock_user_1_data)
	if err != nil {
		t.Error(err)
	}
}

func TestGetStruct(t *testing.T) {
	client := GetClient()
	var data user_struct
	err := client.GetStruct(user_collection_name, mock_user_1_key, &data)
	if err != nil {
		t.Error(err)
	}
}
