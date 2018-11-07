package collection

import (
	"fmt"
	"github.com/teejays/gofiledb/key"
	"sort"
	"strings"
)

var ErrIndexNotImplemented error = fmt.Errorf("Searching is only supported on indexed fields. No index found on one of the fields")

/********************************************************************************
* E N T I T Y
*********************************************************************************/

type QueryPlan struct {
	Query          string
	ConditionsPlan QueryConditionsPlan
}

type QueryConditionsPlan []QueryCondition

type QueryCondition struct {
	FieldLocator    string
	ConditionValues []string
	QueryPosition   int
	HasIndex        bool
	IndexInfo       *IndexInfo
}

func (qs QueryConditionsPlan) Len() int {
	return len(qs)
}

func (qs QueryConditionsPlan) Less(i, j int) bool {
	if qs[i].HasIndex && !qs[j].HasIndex {
		return true
	}
	if !qs[i].HasIndex && qs[j].HasIndex {
		return false
	}
	if qs[i].HasIndex && qs[j].HasIndex {
		return qs[i].IndexInfo.NumValues <= qs[j].IndexInfo.NumValues
	}
	// both don't have indexes, doesn't matter, return something arbitrary e.g. which one was mentioned first in the query
	return qs[i].QueryPosition > qs[j].QueryPosition
}

func (qs QueryConditionsPlan) Swap(i, j int) {
	var temp QueryCondition = qs[i]
	qs[i] = qs[j]
	qs[j] = temp
}

/********************************************************************************
* S E A R C H
*********************************************************************************/

// e.g query: UserId=1+Org.OrgId=1|261+Name=Talha
func (cl *Collection) Search(query string) ([]interface{}, error) {

	// Plan
	plan, err := cl.getQueryPlan(query)
	if err != nil {
		return nil, err
	}

	// Execute the plan
	keys, err := cl.getKeysForQueryConditionPlan(plan.ConditionsPlan)
	if err != nil {
		return nil, err
	}
	// After this for loop, we should have a map of all the doc keys we want to return

	var results []interface{}
	for k := range keys {
		var doc map[string]interface{}
		err := cl.GetIntoStruct(k, &doc)
		if err != nil {
			return nil, err
		}
		results = append(results, doc)
	}

	return results, nil

}

/********************************************************************************
* P L A N
*********************************************************************************/

func (cl *Collection) getQueryPlan(query string) (QueryPlan, error) {

	var err error
	var plan QueryPlan
	plan.Query = query

	plan.ConditionsPlan, err = cl.getConditionsPlanForQuery(query)
	if err != nil {
		return plan, err
	}

	// Todo: Implement Order by plan...

	return plan, nil

}

// This could be way more advanced, but have to make a call on what functionality to allow right now
// Allowed: ANDs: represented by '+'
func (cl *Collection) getConditionsPlanForQuery(query string) (QueryConditionsPlan, error) {

	var err error
	var conditionsPlan QueryConditionsPlan
	const AND_SEPARATOR string = "+"
	const KV_SEPARATOR string = ":"

	// Split each query by the separator `+`, each part represents a separate conditional
	qParts := strings.Split(query, AND_SEPARATOR)

	// for each of the condition's field locator, we'll get and cache the index info so we don't have to do it again
	var indexInfoCache map[string]IndexInfo = make(map[string]IndexInfo)

	// Each part is a condition statement, euch as UserId=12, OrgId=22.
	for i, qP := range qParts {

		// We need to split it by field locator and the condition value
		// Understand this part of condition
		_qP := strings.SplitN(qP, KV_SEPARATOR, 2)
		if len(_qP) < 2 {
			return conditionsPlan, fmt.Errorf("Invalid Query around `%s`", qP)
		}
		fieldLocator := _qP[0]
		fieldCondition := _qP[1]

		var condition QueryCondition
		condition.FieldLocator = fieldLocator
		condition.ConditionValues = []string{fieldCondition}
		condition.QueryPosition = i
		condition.HasIndex = cl.isIndexExist(fieldLocator)

		if condition.HasIndex {
			idxInfo, inCache := indexInfoCache[fieldLocator]
			if !inCache {
				idxInfo, err = cl.getIndexInfo(fieldLocator)
				if err != nil {
					return conditionsPlan, err
				}
				indexInfoCache[fieldLocator] = idxInfo
			}

			condition.IndexInfo = &idxInfo
		}

		conditionsPlan = append(conditionsPlan, condition)

	}

	// by this point, we should have info on all conditional statements...
	// we should order the conditionals based on ... 1) if they have index, 2) how big in the index
	// this is done by the sort method
	sort.Sort(conditionsPlan)

	return conditionsPlan, nil

}

/********************************************************************************
* E X E C U T E
*********************************************************************************/

func (cl *Collection) getKeysForQueryConditionPlan(cPlan QueryConditionsPlan) (map[key.Key]bool, error) {

	var resultKeys map[key.Key]bool = make(map[key.Key]bool) // value type int is just arbitrary so we can store some temp info when find intersects later

	for step, condition := range cPlan {

		step++ // so we start with step = 1

		// if index, open index
		if condition.HasIndex {
			idx, err := cl.loadIndex(condition.FieldLocator)
			if err != nil {
				return nil, err
			}

			for _, conditionValue := range condition.ConditionValues {

				// for each condition, get the values (doc keys) that satisfy the condition
				keys := idx.ValueKeys[conditionValue]
				if step == 1 {
					// first time we're getting the keys, just add them to results
					for _, k := range keys {
						resultKeys[k] = true
					}

				} else {
					resultKeys = findIntersectingKeysOfMapSlice(resultKeys, keys)
				}

			}

		} else { // If there is no index, then we'll have to open all the docs.. :/ Let's not support it for now
			//return nil, fmt.Errorf("Searching is only supported on indexed fields. No index found for field %s", qCondition.FieldLocator)
			return nil, ErrIndexNotImplemented

		}

	}

	return resultKeys, nil

}

// find intersection of a and b
func findIntersectingKeysOfMapSlice(a map[key.Key]bool, b []key.Key) map[key.Key]bool {

	var intersect map[key.Key]bool = make(map[key.Key]bool)
	// loop through the bs, add them to intersect if they are in a
	for _, bVal := range b {
		if hasKey := a[bVal]; hasKey {
			intersect[bVal] = true
		}
	}

	return intersect
}
