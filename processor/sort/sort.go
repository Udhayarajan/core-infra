package sort

import (
	"core-infra/common"
)

var FuncMapper = map[string]func(a, b *common.CSV) bool{
	"sorted_id":        byID,
	"sorted_name":      byName,
	"sorted_continent": byContinent,
}

func byID(a, b *common.CSV) bool {
	return a.ID() < b.ID()
}

func byName(a, b *common.CSV) bool {
	return a.Name() < b.Name()
}

func byContinent(a, b *common.CSV) bool {
	return a.Continent() < b.Continent()
}
