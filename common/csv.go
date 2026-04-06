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
		csv.ensureParsed(-1)
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
	c.ensureParsed(0)
	return c.parsed.ID
}

func (c *CSV) Name() string {
	c.ensureParsed(1)
	return c.parsed.Name
}

func (c *CSV) Address() string {
	c.ensureParsed(2)
	return c.parsed.Address
}

func (c *CSV) Continent() string {
	c.ensureParsed(3)
	return c.parsed.Continent
}

func (c *CSV) ensureParsed(field int) {
	res := c.parsed
	missing := false
	if res == nil {
		missing = true
	} else {
		switch field {
		case 0:
			missing = res.ID == 0
		case 1:
			missing = res.Name == ""
		case 2:
			missing = res.Continent == ""
		default:
			missing = res.Address != ""
		}
	}
	if missing {
		if field < 0 || field > 3 {
			c.parsed = c.parseCSV()
			return
		}
		if res == nil {
			res = &parsedCSV{}
		}
		start, end := c.fieldBounds(field)
		if field == 0 {
			for j := start; j < end; j++ {
				res.ID = res.ID*10 + int32(c.b[j]-'0')
			}
		}

		c.parsed = res
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

// find nth comma position
func (c *CSV) fieldBounds(field int) (start, end int) {
	start = 0
	commaCount := 0

	for i, ch := range c.b {
		if ch == ',' {
			if commaCount == field {
				end = i
				return
			}
			commaCount++
			start = i + 1
		}
	}

	// last field
	if commaCount == field {
		end = len(c.b)
		return
	}

	return -1, -1
}
