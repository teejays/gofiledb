// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"fmt"
	"github.com/teejays/clog"
	"github.com/teejays/gofiledb/collection"
	"github.com/teejays/gofiledb/key"
	"github.com/teejays/gofiledb/util"
	"log"
	"os/user"
	"reflect"
	"testing"
)

const REMOVE_COLLECTION = true
const DESTROY = false

/********************************************************************************
* M O C K  D A T A 																*
*********************************************************************************/

type User struct {
	UserId  int
	Name    string
	Address string
	Age     int
	Org     OrgData
}
type OrgData struct {
	OrgId int64
}

var mockInitOptions1 ClientInitOptions = ClientInitOptions{
	OverwritePreviousData: true,
}
var mockInitOptions2 ClientInitOptions = ClientInitOptions{
	OverwritePreviousData: false,
}

var userCollectionName string = "users"

var userCollectionProps collection.CollectionProps = collection.CollectionProps{
	Name:                  "Users",
	EncodingType:          collection.ENCODING_JSON,
	EnableGzipCompression: false,
	NumPartitions:         3,
}

var mock_user_1_key key.Key = 1
var mock_user_1_data User = User{
	UserId:  1234,
	Name:    "John Doe",
	Address: "123 Main Street, ME 12345",
	Age:     25,
	Org:     OrgData{1},
}

var mock_user_1b_key key.Key = 1
var mock_user_1b_data User = User{
	UserId:  1234,
	Name:    "John Doe B",
	Address: "123 Main Street, ME 12345",
	Age:     30,
	Org:     OrgData{1},
}

var mock_user_2_key key.Key = 2
var mock_user_2_data User = User{
	UserId:  493,
	Name:    "Jane Does",
	Address: "123 Main Street, ME 12345",
	Age:     25,
	Org:     OrgData{261},
}

var mock_user_3_key key.Key = 3
var mock_user_3_data User = User{
	UserId:  973,
	Name:    "Joe Dies",
	Address: "123 Main Street, ME 12345",
	Age:     26,
	Org:     OrgData{1},
}

type Org struct {
	OrgId     int64
	Name      string
	Employees int
}

var orgCollectionProps collection.CollectionProps = collection.CollectionProps{
	Name:                  "Org",
	EncodingType:          collection.ENCODING_JSON,
	EnableGzipCompression: true,
	NumPartitions:         3,
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

var documentRoot string

func init() {
	clog.LogLevel = 0

	usr, err := user.Current()
	if err != nil {
		log.Fatalf("[Init] %v", err)
	}

	documentRoot = util.JoinPath(usr.HomeDir, "gofiledb_test")

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
	mockInitOptions1.DocumentRoot = documentRoot

	err := Initialize(mockInitOptions1)
	if err != nil {
		log.Fatalf("[TestInitClient] %v", err)
	}

	_ = GetClient()

}

// TestInitializeClient: Makes sure we can initialize a fresh copy of a client at documentRoot
func TestInitializeClientTwo(t *testing.T) {
	clog.Infof("Running: TestInitializeClientTwo")
	mockInitOptions2.DocumentRoot = documentRoot

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
	clog.Infof("Running: TestGetClient")
	_ = GetClient()
}

/*
 * Collection Tests
 */

func TestIsCollectionExistFail(t *testing.T) {
	clog.Infof("Running: TestIsCollectionExistFail")
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
	clog.Infof("Running: TestAddCollectionOne")
	client := GetClient()

	err := client.AddCollection(userCollectionProps)
	if err != nil {
		t.Error(err)
	}
}

func TestIsCollectionExist(t *testing.T) {
	clog.Infof("Running: TestIsCollectionExist")
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
	clog.Infof("Running: TestAddIndex")
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
	clog.Infof("Running: TestSetStructFirst")
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
	clog.Infof("Running: TestSetStructSecond")
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
	clog.Infof("Running: TestSetStructThird")
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

func assertUserDataByKey(key key.Key, expectedData interface{}) error {
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
	if err != nil && err != collection.ErrIndexNotImplemented {
		t.Error(err)
	}
	if err != collection.ErrIndexNotImplemented {
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

func TestGzipCollection(t *testing.T) {
	// Create a new collection
	client := GetClient()
	err := client.AddCollection(orgCollectionProps)
	if err != nil {
		t.Error(err)
	}

	// Add Document 1
	err = client.SetStruct(orgCollectionProps.Name, 1, mockOrgs[0])
	if err != nil {
		t.Error(err)
	}

	// Add Index
	err = client.AddIndex(orgCollectionProps.Name, "Employees")
	if err != nil {
		t.Error(err)
	}

	// Add Document 2
	err = client.SetStruct(orgCollectionProps.Name, 2, mockOrgs[1])
	if err != nil {
		t.Error(err)
	}

	// Fetch Document 1
	var data1 Org
	err = fetchAndAssertData(orgCollectionProps.Name, 1, &data1, mockOrgs[0])
	if err != nil {
		t.Error(err)
	}

	// Fetch Document 2
	var data2 Org
	err = fetchAndAssertData(orgCollectionProps.Name, 2, &data2, mockOrgs[1])
	if err != nil {
		t.Error(err)
	}

	// Search
	resp, err := client.Search(orgCollectionProps.Name, "Employees:500")
	if err != nil {
		t.Error(err)
	}
	err = assertSearchResponse(resp, 1, []Org{mockOrgs[1]}, "OrgId")
	if err != nil {
		t.Error(err)
	}

}

func assertSearchResponse(resp SearchResponse, expectedLength int, expectedResult interface{}, keyFieldName string) error {
	if resp.NumDocuments != expectedLength {
		return fmt.Errorf("number of results returned %d do not match the expected number %d", resp.NumDocuments, expectedLength)
	}

	var seen map[key.Key]bool = make(map[key.Key]bool)

	switch reflect.TypeOf(expectedResult).Kind() {
	case reflect.Slice:
		expectedResultV := reflect.ValueOf(expectedResult)

		for i := 0; i < expectedResultV.Len(); i++ {
			dv := expectedResultV.Index(i)
			dkv := dv.FieldByName(keyFieldName)
			var dk key.Key = key.Key(dkv.Int())

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
				rk := key.Key(_rk)

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

func fetchAndAssertData(collectionName string, key key.Key, newData interface{}, expectedData interface{}) error {
	client := GetClient()

	var data Org
	err := client.GetStruct(collectionName, key, &data)
	if err != nil {
		return err
	}
	clog.Debugf("Fetched Struct with Key: %d \n%+v", key, data)
	if data != expectedData {
		return fmt.Errorf("Fectched data did not match expected data: \n Fetched: %v \n Expected: %v", data, expectedData)
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
