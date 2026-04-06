package common

type parsedCSV struct {
	ID        int32
	Name      string
	Address   string
	Continent string
}

type CSV struct {
	b      []byte
	parsed *parsedCSV
}

func NewFromBytes(data []byte, parse bool) *CSV {
	csv := &CSV{b: data}
	if parse {
		csv.ensureParsed()
	}
	return csv
}

func (c *CSV) Encode() ([]byte, error) {
	return c.b, nil
}

func (c *CSV) Length() int {
	return len(c.b)
}

func (c *CSV) ID() int32 {
	c.ensureParsed()
	return c.parsed.ID
}

func (c *CSV) Name() string {
	c.ensureParsed()
	return c.parsed.Name
}

func (c *CSV) Address() string {
	c.ensureParsed()
	return c.parsed.Address
}

func (c *CSV) Continent() string {
	c.ensureParsed()
	return c.parsed.Continent
}

func (c *CSV) ensureParsed() {
	if c.parsed == nil {
		c.parsed = c.parseCSV()
	}
}

func (c *CSV) parseCSV() *parsedCSV {
	var res parsedCSV

	field := 0
	start := 0

	for i, ch := range c.b {
		if ch == ',' {
			switch field {
			case 0:
				for j := start; j < i; j++ {
					res.ID = res.ID*10 + int32(c.b[j]-'0')
				}
			case 1:
				res.Name = string(c.b[start:i])
			case 2:
				res.Address = string(c.b[start:i])
			}
			field++
			start = i + 1
		}
	}

	// last field (continent)
	if field == 3 {
		res.Continent = string(c.b[start:])
	}

	return &res
}

func (c *CSV) Bytes() []byte {
	return c.b
}
