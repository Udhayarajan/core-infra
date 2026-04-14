package data

import (
	"math/rand"
	"strconv"
	"sync"

	"core-infra/common"
)

const (
	minNameLength              = 10
	maxNameLength              = 15
	minAddressLength           = 15
	maxAddressLength           = 20
	alphabetCharset            = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	alphabetNumberSpaceCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
)

type Continent string

var (
	Asia         Continent = "Asia"
	Africa       Continent = "Africa"
	Australia    Continent = "Australia"
	Europe       Continent = "Europe"
	NorthAmerica Continent = "North America"
	SouthAmerica Continent = "South America"
)

var availableContinents = []Continent{
	Asia,
	Africa,
	Australia,
	Europe,
	NorthAmerica,
	SouthAmerica,
}

var bufPool = sync.Pool{
	New: func() any { return make([]byte, 0, 64) },
}

type Generator struct {
	rng *rand.Rand
}

func NewGenerator(rng *rand.Rand) *Generator {
	return &Generator{rng: rng}
}

func (g *Generator) Generate() *common.CSV {
	b := bufPool.Get().([]byte)[:0]

	b = strconv.AppendInt(b, int64(g.rng.Int31()), 10)
	b = append(b, ',')
	b = g.appendRandomString(b, minNameLength, maxNameLength, alphabetCharset)
	b = append(b, ',')
	b = g.appendRandomString(b, minAddressLength, maxAddressLength, alphabetNumberSpaceCharset)
	b = append(b, ',')
	b = append(b, availableContinents[g.rng.Intn(len(availableContinents))]...)

	// Copy out before returning to pool
	out := make([]byte, len(b))
	copy(out, b)
	bufPool.Put(b)
	return common.NewFromBytes(out, false)
}

func (g *Generator) appendRandomString(b []byte, min, max int, charset string) []byte {
	length := min + g.rng.Intn(max-min+1)
	for i := 0; i < length; i++ {
		b = append(b, charset[g.rng.Intn(len(charset))])
	}
	return b
}
