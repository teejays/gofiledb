package gofiledb

import (
	"fmt"
	"github.com/teejays/clog"
	"github.com/teejays/gofiledb/key"
	"github.com/teejays/gofiledb/util"
	"log"
	"os"
	"strings"
	"sync"
)

type BoolAtomic struct {
	Val bool
	sync.RWMutex
}

func (a *BoolAtomic) GetVal() bool {
	a.RLock()
	val := a.Val
	a.RUnlock()
	return val
}

func (a *BoolAtomic) SetVal(v bool) {
	a.Lock()
	a.Val = v
	a.Unlock()
}

func (a *BoolAtomic) CompareAndSet(v bool) bool {
	a.Lock()
	var success bool // whether we've succesfully swapped the value
	if a.Val != v {
		a.Val = v
		success = true
	}
	a.Unlock()
	return success
}

// Let's make a call that we can refragament only one collection at a time.
var isRepartitioning BoolAtomic

type RepartitionParams struct {
	DataDirectory    string // the location of the folder which stores the partition folders
	NumPartitionsNew int    // the number of partitions that we want
}

var ErrIsRepartitioning = fmt.Errorf("The system is already busy repartitioning a collection. Please try again in a while.")

func Repartition(params RepartitionParams) error {

	if !((&isRepartitioning).CompareAndSet(true)) {
		return ErrIsRepartitioning
	}

	if strings.TrimSpace(params.DataDirectory) == "" {
		return fmt.Errorf("invalid data directory provided: %s", params.DataDirectory)
	}

	if params.NumPartitionsNew < 1 {
		log.Panicf("invalid num-partitions provided: %d", params.NumPartitionsNew)
	}

	// get all the current partition folders so we can read into them and start moving files
	partitionFolders, err := getSubfiles(params.DataDirectory)
	if err != nil {
		return err
	}

	// for each partition folder, go inside, copy and move all the files to their new locations
	for _, partition := range partitionFolders {

		path := util.JoinPath(params.DataDirectory, partition)
		// Ensure that we're looking into a folder, and not a file.
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			clog.Warnf("Repartition: found a non-directory file `%s` at %s. Expected to find only partition folders", partition, params.DataDirectory)
			continue
		}

		// From the folder, get all the files
		files, err := getSubfiles(path)
		if err != nil {
			return err
		}
		for _, f := range files {
			// Ensure that we're looking at a file, and not a dir.
			info, err := os.Stat(util.JoinPath(path, f))
			if err != nil {
				return err
			}
			if info.IsDir() {
				clog.Warnf("Repartition: found a directory `%s` at %s. Expected to find only documents files", f, path)
				continue
			}

			// What should be teh new path of this file? Get the new partition name
			// but first we need the Key for this file
			k, err := key.GetKeyFromFileName(f)
			if err != nil {
				return err
			}

			newPartitionDir := k.GetPartitionDirName(params.NumPartitionsNew)
			oldPath := util.JoinPath(params.DataDirectory, partition)
			newPath := util.JoinPath(params.DataDirectory, newPartitionDir)

			// if the dir doesn't exist, create one
			if _, err := os.Stat(newPath); os.IsNotExist(err) {
				fmt.Printf("Creating dir at %s...\n", newPath)
				os.Mkdir(newPath, os.ModePerm)
			}

			// only move/rename if the path/name is different
			oldName := util.JoinPath(oldPath, f)
			newName := util.JoinPath(newPath, f)
			if oldName != newName {
				fmt.Printf("Moving file from %s to %s...\n", oldPath, newPath)
				err := os.Rename(oldName, newName)
				if err != nil {
					return err
				}
			}
		}
	}

	(&isRepartitioning).CompareAndSet(false)

	return nil
}

// getSubfiles returns all the names of the files/directories at a given path
func getSubfiles(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var folders []string
	list, _ := file.Readdirnames(0) // 0 to read all files and folders
	for _, name := range list {
		folders = append(folders, name)
	}

	return folders, nil
}
