package gofiledb

import (
	"github.com/teejays/gofiledb/key"
)

/********************************************************************************
* K E Y
*********************************************************************************/

type Key key.Key

func NewKey(i int64) key.Key {
	return key.Key(i)
}
