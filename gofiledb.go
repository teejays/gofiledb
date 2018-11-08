// gofiledb package provides an interface between Go applications and the linux-based file system
// so that the filesystem can be used as a database or a caching layer.
package gofiledb

import (
	"fmt"
	"github.com/teejays/clog"
	"github.com/teejays/gofiledb/collection"
	"github.com/teejays/gofiledb/util"
	"os"
)

type ClientInitOptions struct {
	DocumentRoot          string
	OverwritePreviousData bool // if true, gofiledb will remove all the existing data in the document root
}

type CollectionProps collection.CollectionProps

const (
	ENCODING_NONE uint = collection.ENCODING_NONE
	ENCODING_JSON uint = collection.ENCODING_JSON
	ENCODING_GOB  uint = collection.ENCODING_GOB
)

var ErrCollectionIsNotExist = collection.ErrCollectionIsNotExist
var ErrCollectionIsExist = collection.ErrCollectionIsExist
var ErrIndexNotImplemented = collection.ErrIndexNotImplemented

// Initialize setsup the package for use by an appliction. This should be called before the client can be used.
func Initialize(p ClientInitOptions) error {
	// Although rare, it is still possible that two almost simultaneous calls are made to the Initialize function,
	// which could end up initializing the client twice and might overwrite the param values. Hence, we use a lock
	// to avoid that situation.
	globalClientLock.Lock()
	defer globalClientLock.Unlock()

	if globalClient.isInitialized {
		return ErrClientAlreadyInitialized
	}

	var cParams ClientParams = NewClientParams(p.DocumentRoot)

	// Ensure that the params provided make sense
	err := cParams.validate()
	if err != nil {
		return err
	}

	// Sanitize the params so they'r emore standard
	cParams = cParams.sanitize()

	var client Client
	client.ClientParams = cParams

	// If overwrite previousdata flag is passed, we should delete existing data at document root
	if p.OverwritePreviousData {
		err = client.Destroy()
		if err != nil {
			return err
		}
	}

	// Create the neccesary folders
	err = util.CreateDirIfNotExist(client.ClientParams.documentRoot)
	if err != nil {
		return err
	}
	err = util.CreateDirIfNotExist(util.JoinPath(client.ClientParams.documentRoot, util.DATA_DIR_NAME))
	if err != nil {
		return err
	}
	err = util.CreateDirIfNotExist(util.JoinPath(client.ClientParams.documentRoot, util.META_DIR_NAME))
	if err != nil {
		return err
	}

	// Check if we already have a client that is intitilzed at this Document Root
	err = client.getMeta("globalClient.gob", &client)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// By this point, either the existing client has been loaded to client var, or not.
	// If client.isInitialized == true, then the existing client has been loaded.
	if client.isInitialized {
		clog.Warnf("Existing GoFileDb client found at %s. Loading it.", p.DocumentRoot)
		// Ensure that the loaded params match the new params provided
		// For now, the only param that matters is document root.
		if client.documentRoot != p.DocumentRoot {
			return fmt.Errorf("An existing GoFileDb client has been found at the location %s. However, that client's documentRoot is set to %s. This is an unexpected error.", p.DocumentRoot, client.documentRoot)
		}
		if client.collections == nil {
			return fmt.Errorf("An existing GoFileDb client has been found at the location %s. However, that client does not have an initialized collection data. This is an unexpected error.", p.DocumentRoot)
		}

		return nil
	}

	// Code here corresponds to the case when we're creating a new Client
	// Initialize the CollectionStore
	collections := new(collectionStore)                        // collections is a pointer to collectionStore
	collections.Store = make(map[string]collection.Collection) // default case

	client.collections = collections

	client.isInitialized = true

	globalClient = client

	err = (&globalClient).save()
	if err != nil {
		return err
	}

	return nil
}

func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}
