package gofiledb

import (
	"fmt"
	"sort"
	"strings"
)

/********************************************************************************
* S E A R C H
*********************************************************************************/

type queryPlan struct {
	Query          string
	ConditionsPlan queryConditionsPlan
}

type queryConditionsPlan []queryPlanCondition

type queryPlanCondition struct {
	FieldLocator    string
	ConditionValues []string
	QueryPosition   int
	HasIndex        bool
	IndexInfo       *IndexInfo
}

func (qs queryConditionsPlan) Len() int {
	return len(qs)
}

func (qs queryConditionsPlan) Less(i, j int) bool {
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

func (qs queryConditionsPlan) Swap(i, j int) {
	var temp queryPlanCondition = qs[i]
	qs[i] = qs[j]
	qs[j] = temp
}

// Todo: add order by
// e.g query: UserId=1+Org.OrgId=1|261+Name=Talha
func (cl *Collection) search(query string) ([]interface{}, error) {
	var err error
	var qPlan queryPlan
	qPlan.Query = query

	// get the plan, which is in the form of type queryConditionsPlan
	qPlan.ConditionsPlan, err = cl.getConditionsPlan(query)
	if err != nil {
		return nil, err
	}

	// execute the plan
	var resultKeys map[string]bool // value type int is just arbitrary so we can store some temp info when find intersects later
	for step, qCondition := range qPlan.ConditionsPlan {
		step++ // so we start with step = 1

		// if index, open index
		if qCondition.HasIndex {
			idx, err := cl.getIndex(qCondition.FieldLocator)
			if err != nil {
				return nil, err
			}

			for _, conditionValue := range qCondition.ConditionValues {

				// for each condition, get the values (doc keys) that satisfy the condition
				docIds := idx.KeyValue[conditionValue]
				if step == 1 {
					// first time we're getting the docs, just add them to results
					for _, dId := range docIds {
						resultKeys[dId] = true
					}

				} else {
					resultKeys = findIntersectionMapSlice(resultKeys, docIds)
				}

			}

		} else { // If there is no index, then we'll have to open all the docs.. :/ Let's not support it for now
			return nil, fmt.Errorf("Searching is only supported on indexed fields. No index found for field %s", qCondition.FieldLocator)

		}

	}

	// After this for loop, we should have a map of all the doc keys we want to return

	var results []interface{}
	for docKey, _ := range resultKeys {
		var doc map[string]interface{}
		err := cl.getIntoStruct(docKey, &doc)
		if err != nil {
			return nil, err
		}
		results = append(results, doc)
	}

	return results, nil

}

// find intersection of a and b
func findIntersectionMapSlice(a map[string]bool, b []string) map[string]bool {

	var intersect map[string]bool = make(map[string]bool)
	// loop through the bs, add them to intersect if they are in a
	for _, bVal := range b {
		if hasKey := a[bVal]; hasKey {
			intersect[bVal] = true
		}
	}

	return intersect
}

// This could be way more advanced, but have to make a call on what functionality to allow right now
// Allowed: ANDs: represented by '+'
func (cl *Collection) getConditionsPlan(query string) (queryConditionsPlan, error) {

	var err error
	var qConditionsPlan queryConditionsPlan
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
		_qP := strings.SplitN(qP, KV_SEPARATOR, 1)
		if len(_qP) < 2 {
			return qConditionsPlan, fmt.Errorf("Invalid Query around `%s`", qP)
		}
		fieldLocator := _qP[0]
		fieldCondition := _qP[1]

		var qPlanCondition queryPlanCondition
		qPlanCondition.FieldLocator = fieldLocator
		qPlanCondition.ConditionValues = []string{fieldCondition}
		qPlanCondition.QueryPosition = i
		qPlanCondition.HasIndex = cl.indexIsExist(fieldLocator)

		if qPlanCondition.HasIndex {
			idxInfo, inCache := indexInfoCache[fieldLocator]
			if !inCache {
				idxInfo, err = cl.getIndexInfo(fieldLocator)
				if err != nil {
					return qConditionsPlan, err
				}
				indexInfoCache[fieldLocator] = idxInfo
			}

			qPlanCondition.IndexInfo = &idxInfo
		}

		qConditionsPlan = append(qConditionsPlan, qPlanCondition)

	}

	// by this point, we should have info on all conditional statements...
	// we should order the conditionals based on ... 1) if they have index, 2) how big in the index
	// this is done by the sort method
	sort.Sort(qConditionsPlan)

	return qConditionsPlan, nil

}
