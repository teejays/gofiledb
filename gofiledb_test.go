// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"fmt"
	"log"
	"os/user"
	"testing"
)

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
		documentRoot:  document_root,
		numPartitions: 50,
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

	props := CollectionProps{
		Name:         userCollectionName,
		EncodingType: ENCODING_GOB,
	}
	err := client.AddCollection(props)
	if err != nil {
		t.Error(err)
	}
}

func TestSetStruct(t *testing.T) {
	client := GetClient()
	err := client.SetStruct(userCollectionName, mock_user_1_key, mock_user_1_data)
	if err != nil {
		t.Error(err)
	}

	err = client.SetStruct(userCollectionName, mock_user_2_key, mock_user_2_data)
	if err != nil {
		t.Error(err)
	}
}

func TestGetStruct(t *testing.T) {
	client := GetClient()
	var data User
	err := client.GetStruct(userCollectionName, mock_user_1_key, &data)
	if err != nil {
		t.Error(err)
	}
	if data != mock_user_1_data {
		fmt.Printf("%v", data)
		t.Error(fmt.Errorf("Data did not match"))
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

// func TestRemoveCollection(t *testing.T) {
// 	client := GetClient()
// 	err := client.RemoveCollection(userCollectionName)
// 	if err != nil {
// 		t.Error(err)
// 	}
// }
