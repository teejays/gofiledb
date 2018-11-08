// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"fmt"
	"github.com/teejays/clog"
	"github.com/teejays/gofiledb/util"
	"log"
	"os/user"
	"reflect"
	"testing"
)

const REMOVE_COLLECTION = false
const DESTROY = false

var documentRoot string

func init() {
	// Set this to 0 for full logging, and 7 to no logging.
	clog.LogLevel = 0

	usr, err := user.Current()
	if err != nil {
		log.Fatalf("[Init] %v", err)
	}

	documentRoot = util.JoinPath(usr.HomeDir, "gofiledb_test")

}

/********************************************************************************
* M O C K  D A T A 																*
*********************************************************************************/

type (
	User struct {
		UserId  int
		Name    string
		Address string
		Age     int
		Org     OrgData
	}
	OrgData struct {
		OrgId int64
	}
)

type Org struct {
	OrgId     int
	Name      string
	Employees int
}

var mockClients []ClientInitOptions = []ClientInitOptions{
	// 0
	{OverwritePreviousData: true},
	// 1
	{OverwritePreviousData: true},
}

var mockCollections map[string]CollectionProps = map[string]CollectionProps{
	"User": CollectionProps{
		Name:                  "User",
		EncodingType:          ENCODING_JSON,
		EnableGzipCompression: false,
		NumPartitions:         3,
	},
	"Org": CollectionProps{
		Name:                  "Org",
		EncodingType:          ENCODING_JSON,
		EnableGzipCompression: true,
		NumPartitions:         3,
	},
}

var mockUsers map[string]User = map[string]User{
	// Mock 0 A
	"1": User{
		UserId:  1,
		Name:    "John Doe",
		Address: "123 Main Street, ME 12345",
		Age:     25,
		Org:     OrgData{OrgId: 1},
	},
	// Mock 1a
	"1a": User{
		UserId:  1,
		Name:    "John Doe B",
		Address: "123 Main Street, ME 12345",
		Age:     30,
		Org:     OrgData{OrgId: 1},
	},
	// Mock 1 A
	"2": User{
		UserId:  2,
		Name:    "Jane Does",
		Address: "123 Main Street, ME 12345",
		Age:     25,
		Org:     OrgData{OrgId: 261},
	},
	// Mock 2 A
	"3": User{
		UserId:  3,
		Name:    "Joe Dies",
		Address: "123 Main Street, ME 12345",
		Age:     26,
		Org:     OrgData{OrgId: 1},
	},
}

var mockOrgs []Org = []Org{
	Org{
		OrgId:     1,
		Name:      "Company A",
		Employees: 100,
	},
	Org{
		OrgId:     2,
		Name:      "Company B",
		Employees: 500,
	},
}

/********************************************************************************
* T E S T S 																	*
*********************************************************************************/

/*
 * Client Tests
 */

// TestGetClientPreInit: Makes sure we get a ClientNotInitialized Error when getting a client which has not been initialized
func TestGetClientPreInit(t *testing.T) {
	clog.Infof("Running: TestGetClientPreInit")
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
	clog.Infof("Running: TestInitializeClient")

	mockClients[0].DocumentRoot = documentRoot
	err := Initialize(mockClients[0])
	if err != nil {
		log.Fatalf("[TestInitClient] %v", err)
	}

	_ = GetClient() // ensure that this doesn't panic

}

// TestInitializeClient: Makes sure we can initialize a fresh copy of a client at documentRoot
func TestInitializeClientAgain(t *testing.T) {
	clog.Infof("Running: TestInitializeClientTwo")

	mockClients[1].DocumentRoot = documentRoot
	err := Initialize(mockClients[1])
	if err != nil && err != ErrClientAlreadyInitialized {
		log.Fatalf("[TestInitClient] %v", err)
	}
	if err == nil {
		t.Error("Expected ErrClientAlreadyInitialized error but got nil")
	}

	_ = GetClient() // Ensure that this doesn't panic

}

// TestGetClient: Makes sure we can get the initialized client
func TestGetClient(t *testing.T) {
	clog.Infof("Running: TestGetClient")
	_ = GetClient() // Ensure that this doesn't panic
}

/*
 * Collection Tests
 */

func TestIsCollectionExistFail(t *testing.T) {
	clog.Infof("Running: TestIsCollectionExistFail")

	client := GetClient()
	exists, err := client.IsCollectionExist(mockCollections["User"].Name)
	if err != nil {
		t.Error(err)
	}
	if exists {
		t.Error("Expected collection to not exist, but it exists")
	}
}

func TestAddCollection(t *testing.T) {
	clog.Infof("Running: TestAddCollectionUser")
	client := GetClient()

	err := client.AddCollection(mockCollections["User"])
	if err != nil {
		t.Error(err)
	}
}

func TestIsCollectionExist(t *testing.T) {
	clog.Infof("Running: TestIsCollectionExist")
	client := GetClient()
	exists, err := client.IsCollectionExist(mockCollections["User"].Name)
	if err != nil {
		t.Error(err)
	}
	if !exists {
		t.Errorf("Expected collections %s to exist but received false for IsCollectionExist method", mockCollections["User"].Name)
	}
}

/*
 * Index Tests
 */

func TestAddIndex(t *testing.T) {
	clog.Infof("Running: TestAddIndex")
	client := GetClient()
	err := client.AddIndex(mockCollections["User"].Name, "Age")
	if err != nil {
		t.Error(err)
	}

	err = client.AddIndex(mockCollections["User"].Name, "Org.OrgId")
	if err != nil {
		t.Error(err)
	}
}

/*
 * Data Write
 */

func TestSetStructFirst(t *testing.T) {
	clog.Infof("Running: TestSetStructFirst")

	collectionName := "User"
	ref := "1"
	data := mockUsers[ref]
	key := Key(data.UserId)

	client := GetClient()
	err := client.SetStruct(mockCollections[collectionName].Name, key, data)
	if err != nil {
		t.Error(err)
	}

	var newData User
	err = fetchAndAssertData(collectionName, key, newData, data, "UserId")
	if err != nil {
		t.Error(err)
	}
}

func TestSetStructSecond(t *testing.T) {
	clog.Infof("Running: TestSetStructSecond")

	collectionName := "User"
	ref := "2"
	data := mockUsers[ref]
	key := Key(data.UserId)

	client := GetClient()
	err := client.SetStruct(mockCollections[collectionName].Name, key, data)
	if err != nil {
		t.Error(err)
	}

	var newData User
	err = fetchAndAssertData(collectionName, key, newData, data, "UserId")
	if err != nil {
		t.Error(err)
	}
}

func TestSetStructThird(t *testing.T) {
	clog.Infof("Running: TestSetStructThird")

	collectionName := "User"
	ref := "3"
	data := mockUsers[ref]
	key := Key(data.UserId)

	client := GetClient()
	err := client.SetStruct(mockCollections[collectionName].Name, key, data)
	if err != nil {
		t.Error(err)
	}

	var newData User
	err = fetchAndAssertData(collectionName, key, newData, data, "UserId")
	if err != nil {
		t.Error(err)
	}
}

func TestSetSructOverwrite(t *testing.T) {
	clog.Infof("Running: TestSetStructOverWrite")

	collectionName := "User"
	ref := "1a"
	data := mockUsers[ref]
	key := Key(data.UserId)

	client := GetClient()
	err := client.SetStruct(mockCollections[collectionName].Name, key, data)
	if err != nil {
		t.Error(err)
	}

	var newData User
	err = fetchAndAssertData(collectionName, key, newData, data, "UserId")
	if err != nil {
		t.Error(err)
	}
}

/*
 * Data Read
 */

func TestGetStruct(t *testing.T) {
	clog.Infof("Running: TestGetStruct")

	collectionName := "User"
	ref := "1a"
	data := mockUsers[ref]
	key := Key(data.UserId)

	var newData User
	err := fetchAndAssertData(collectionName, key, newData, data, "UserId")
	if err != nil {
		t.Error(err)
	}
}

/*
 * Data Search
 */

func TestSearch(t *testing.T) {
	collectionName := "User"
	keyField := "UserId"

	c := GetClient()
	resp, err := c.Search(collectionName, "Age:25")
	if err != nil {
		t.Error(err)
	}
	err = assertSearchResponse(resp, 1, []User{mockUsers["2"]}, keyField)
	if err != nil {
		fmt.Println(resp)
		t.Error(err)
	}

	resp, err = c.Search(collectionName, "Org.OrgId:1")
	if err != nil {
		t.Error(err)
	}
	err = assertSearchResponse(resp, 2, []User{mockUsers["1a"], mockUsers["3"]}, keyField)
	if err != nil {
		fmt.Println(resp)
		t.Error(err)
	}

	resp, err = c.Search(collectionName, "Org.OrgId:1+Age:26")
	if err != nil {
		t.Error(err)
	}
	err = assertSearchResponse(resp, 1, []User{mockUsers["3"]}, keyField)
	if err != nil {
		fmt.Println(resp)
		t.Error(err)
	}

	resp, err = c.Search(collectionName, "Org.OrgId:1+Age:26+Name:Tom")
	if err != nil && err != ErrIndexNotImplemented {
		t.Error(err)
	}
	if err != ErrIndexNotImplemented {
		t.Error(fmt.Errorf("Expected ErrIndexNotImplemented got: %v, %s", resp, err))
	}

}

func TestGzipCollection(t *testing.T) {
	collectionName := "Org"
	collectionProps := mockCollections[collectionName]
	keyField := "OrgId"

	// Create a new collection
	client := GetClient()
	err := client.AddCollection(collectionProps)
	if err != nil {
		t.Error(err)
	}

	// Add Document 1
	ref := 0
	data := mockOrgs[ref]
	key := data.OrgId
	err = client.SetStruct(collectionName, Key(key), data)
	if err != nil {
		t.Error(err)
	}

	// Add Index
	err = client.AddIndex(collectionName, "Employees")
	if err != nil {
		t.Error(err)
	}

	// Add Document 2
	ref = 1
	data = mockOrgs[ref]
	key = data.OrgId
	err = client.SetStruct(collectionName, Key(key), data)
	if err != nil {
		t.Error(err)
	}

	// Fetch Document 1
	ref = 0
	data = mockOrgs[ref]
	key = data.OrgId

	var data1 Org
	err = fetchAndAssertData(collectionName, Key(key), data1, data, keyField)
	if err != nil {
		t.Error(err)
	}

	// Fetch Document 2
	ref = 1
	data = mockOrgs[ref]
	key = data.OrgId

	var data2 Org
	err = fetchAndAssertData(collectionName, Key(key), data2, data, keyField)
	if err != nil {
		t.Error(err)
	}

	// Search
	resp, err := client.Search(collectionName, "Employees:500")
	if err != nil {
		t.Error(err)
	}
	err = assertSearchResponse(resp, 1, []Org{mockOrgs[1]}, keyField)
	if err != nil {
		t.Error(err)
	}

}

func TestRemoveCollection(t *testing.T) {
	if !REMOVE_COLLECTION {
		log.Println("REMOVE_COLLECTION flag set to false. Leaving collection data as it is.")
		return
	}
	client := GetClient()
	err := client.RemoveCollection(mockCollections["User"].Name)
	if err != nil {
		t.Error(err)
	}

	err = client.RemoveCollection(mockCollections["Org"].Name)
	if err != nil {
		t.Error(err)
	}
}

func TestDestroy(t *testing.T) {
	if !DESTROY {
		log.Println("DESTROY flag set to false. Not destorying the db.")
		return
	}
	client := GetClient()
	err := client.Destroy()
	if err != nil {
		t.Error(err)
	}
}

/********************************************************************************
* H E L P E R S
*********************************************************************************/

// func assertUserDataByKey(key Key, expectedData interface{}) error {
// 	client := GetClient()

// 	var data User
// 	err := client.GetStruct(userCollectionName, key, &data)
// 	if err != nil {
// 		return err
// 	}
// 	if data != expectedData {
// 		return fmt.Errorf("Fectched data did not match expected data: \n Fetched: %v \n Expected: %v", data, expectedData)
// 	}

// 	return nil
// }

func fetchAndAssertData(collectionName string, key Key, newData interface{}, expectedData interface{}, keyFieldName string) error {
	client := GetClient()

	err := client.GetStruct(collectionName, key, &newData)
	if err != nil {
		return err
	}
	clog.Debugf("Fetched with Key: %d \n%+v", key, newData)
	clog.Debugf("Expected with Key: %d \n%+v", key, expectedData)

	err = assertEquality(expectedData, newData, keyFieldName)
	if err != nil {
		return fmt.Errorf("Fectched data did not match expected data: \n Fetched: %v \n Expected: %v \n Error: %s", newData, expectedData, err)
	}

	return nil
}

func assertSearchResponse(resp SearchResponse, expectedLength int, expectedResult interface{}, keyFieldName string) error {
	if resp.NumDocuments != expectedLength {
		return fmt.Errorf("number of results returned %d do not match the expected number %d", resp.NumDocuments, expectedLength)
	}

	var seen map[Key]bool = make(map[Key]bool)

	switch reflect.TypeOf(expectedResult).Kind() {
	case reflect.Slice:
		expectedResultV := reflect.ValueOf(expectedResult)

		for i := 0; i < expectedResultV.Len(); i++ {
			dv := expectedResultV.Index(i)
			dkv := dv.FieldByName(keyFieldName)
			var dk Key = Key(dkv.Int())

			if seen[dk] == true {
				return fmt.Errorf("Same document can not be used twice in the expected results: %v", dv.Interface())
			}
			seen[dk] = false
			for _, _r := range resp.Result {
				r, ok := _r.(map[string]interface{})
				if !ok {
					return fmt.Errorf("Could not assert a result item as a map. It should be a map...")
				}

				__rk, ok := r[keyFieldName].(float64)
				if !ok {
					return fmt.Errorf("Could not assert the key field '%s' in a result item as a float. It is of type %s. \n%+v", keyFieldName, reflect.TypeOf(r[keyFieldName]), r)
				}

				_rk := int(__rk)
				rk := Key(_rk)

				if dk == rk {
					if seen[dk] == true {
						return fmt.Errorf("Same document seen twice in the results: %v", dk)
					}
					seen[rk] = true
					break
				}
			}
		}
		break
	default:
		return fmt.Errorf("The expected results passed as param is not a slice. It should be a slice.")
	}

	for d, s := range seen {
		if !s {
			return fmt.Errorf("Expected document not found in the results: %v", d)
		}
	}

	return nil
}

func assertEquality(expectedData interface{}, data interface{}, keyFieldName string) error {
	// We get the key field value in both structs and compare them

	dv := reflect.ValueOf(expectedData)
	dkv := dv.FieldByName(keyFieldName)
	var dk Key = Key(dkv.Int()) // dkv should be of type int

	r, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("Could not assert data param as a map. It should be a map...")
	}

	__rk, ok := r[keyFieldName].(float64)
	if !ok {
		return fmt.Errorf("Could not assert the key field '%s' in a result item as a float. It is of type %s. \n%+v", keyFieldName, reflect.TypeOf(r[keyFieldName]), r)
	}

	_rk := int(__rk)
	rk := Key(_rk)

	if dk != rk {
		return fmt.Errorf("The key fields did not match for the two structs: expected %d got %d", dk, rk)
	}

	return nil
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
