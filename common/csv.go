package common

type parsedCSV struct {
	ID        int32  // 0
	Name      string // 1
	Address   string // 2
	Continent string // 3
}

type CSV struct {
	raw       []byte
	parsedCSV *parsedCSV
	parsed    uint8 // bit 0=ID, bit 1=Name, bit 2=Address, bit 3=Continent

}

func NewFromBytes(data []byte, parse bool) *CSV {
	csv := &CSV{raw: data}
	if parse {
		csv.ParseAll()
	}
	return csv
}

func (c *CSV) ParseAll() {
	if c.parsed == 0b00001111 {
		return
	}

	res := &parsedCSV{}

	field := 0
	start := 0

	for i, ch := range c.raw {
		if ch == ',' {
			switch field {
			case 0:
				for j := start; j < i; j++ {
					res.ID = res.ID*10 + int32(c.raw[j]-'0')
				}
			case 1:
				res.Name = string(c.raw[start:i])
			case 2:
				res.Address = string(c.raw[start:i])
			}
			field++
			start = i + 1
		}
	}

	// last field (continent)
	if field == 3 {
		res.Continent = string(c.raw[start:])
	}

	c.parsedCSV = res
	c.parsed = 0b00001111
}

func (c *CSV) Encode() ([]byte, error) {
	return c.raw, nil
}

func (c *CSV) Length() int {
	return len(c.raw)
}

func (c *CSV) Bytes() []byte {
	return append(c.raw, '\n')
}

// accessors just read the field directly — no ParseAll call
// caller is responsible for calling ParseAll before accessing fields

func (c *CSV) ID() int32         { return c.parsedCSV.ID }
func (c *CSV) Name() string      { return c.parsedCSV.Name }
func (c *CSV) Address() string   { return c.parsedCSV.Address }
func (c *CSV) Continent() string { return c.parsedCSV.Continent }
