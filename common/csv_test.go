package common

import (
	"bytes"
	"encoding/csv"
	"reflect"
	"strconv"
	"testing"
)

type encodingCSV struct {
	ID        int32  `csv:"id"`
	Name      string `csv:"name"`
	Address   string `csv:"address"`
	Continent string `csv:"continent"`
}

var (
	benchmarkCSVInput = encodingCSV{
		ID:        12345,
		Name:      "Ada Lovelace",
		Address:   "10 Downing Street",
		Continent: "Europe",
	}
	benchmarkCSVBytes = []byte(
		strconv.FormatInt(int64(benchmarkCSVInput.ID), 10) + "," +
			benchmarkCSVInput.Name + "," +
			benchmarkCSVInput.Address + "," +
			benchmarkCSVInput.Continent,
	)
	benchmarkStdlibCSVBytes = func() []byte {
		var buf bytes.Buffer
		w := csv.NewWriter(&buf)
		_ = w.Write([]string{
			strconv.FormatInt(int64(benchmarkCSVInput.ID), 10),
			benchmarkCSVInput.Name,
			benchmarkCSVInput.Address,
			benchmarkCSVInput.Continent,
		})
		w.Flush()
		return buf.Bytes()
	}()
	benchmarkCSVValue  int32
	benchmarkCSVString string
	benchmarkBytesSink []byte
)

func consumeBenchmarkSinks() {
	_, _, _ = benchmarkCSVValue, benchmarkCSVString, benchmarkBytesSink
}

func Benchmark_CustomCSVEncoding(b *testing.B) {
	b.ReportAllocs()
	csvValue := NewFromBytes(benchmarkCSVBytes, false)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data, err := csvValue.Encode()
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBytesSink = data
	}

	consumeBenchmarkSinks()
}

func Benchmark_StdlibCSVEncoding(b *testing.B) {
	b.ReportAllocs()
	record := []string{
		strconv.FormatInt(int64(benchmarkCSVInput.ID), 10),
		benchmarkCSVInput.Name,
		benchmarkCSVInput.Address,
		benchmarkCSVInput.Continent,
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := csv.NewWriter(&buf)
		if err := w.Write(record); err != nil {
			b.Fatal(err)
		}
		w.Flush()
		benchmarkBytesSink = buf.Bytes()
	}

	consumeBenchmarkSinks()
}

func Benchmark_CustomCSVDecoding(b *testing.B) {
	b.ReportAllocs()
	data := benchmarkCSVBytes
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		csvValue := NewFromBytes(data, false)
		benchmarkCSVValue = csvValue.ID()
		benchmarkCSVString = csvValue.Name()
		benchmarkCSVString = csvValue.Address()
		benchmarkCSVString = csvValue.Continent()
	}

	consumeBenchmarkSinks()
}

func Benchmark_StdlibCSVDecoding(b *testing.B) {
	b.ReportAllocs()
	data := benchmarkStdlibCSVBytes
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := csv.NewReader(bytes.NewReader(data))
		record, err := reader.Read()
		if err != nil {
			b.Fatal(err)
		}
		parsedID, err := strconv.ParseInt(record[0], 10, 32)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCSVValue = int32(parsedID)
		benchmarkCSVString = record[1]
		benchmarkCSVString = record[2]
		benchmarkCSVString = record[3]
	}

	consumeBenchmarkSinks()
}

func TestNewFromBytes(t *testing.T) {
	type args struct {
		data  []byte
		parse bool
	}
	// Note: valid CSV is "id,name,address,continent" (order mismatch as well considered as invalid)
	tests := []struct {
		name string
		args args
		want *CSV
	}{
		{
			name: "valid CSV data with parsing",
			args: args{
				data:  []byte("12345,Ada Lovelace,10 Downing Street,Europe"),
				parse: true,
			},
			want: &CSV{
				raw: []byte("12345,Ada Lovelace,10 Downing Street,Europe"),
				parsedCSV: &parsedCSV{
					ID:        12345,
					Name:      "Ada Lovelace",
					Address:   "10 Downing Street",
					Continent: "Europe",
				},
				parsed: 0b00001111,
			},
		},
		{
			name: "valid CSV data without parsing",
			args: args{
				data:  []byte("12345,Ada Lovelace,10 Downing Street,Europe"),
				parse: false,
			},
			want: &CSV{
				raw: []byte("12345,Ada Lovelace,10 Downing Street,Europe"),
			},
		},
		{
			name: "invalid CSV data with parsing - result in invalid data",
			args: args{
				data:  []byte("Ada Lovelace,10 Downing Street,12345"),
				parse: true,
			},
			want: &CSV{
				raw: []byte("Ada Lovelace,10 Downing Street,12345"),
				parsedCSV: &parsedCSV{
					ID:   -161940601,
					Name: "10 Downing Street",
				},
				parsed: 0b00001111,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewFromBytes(tt.args.data, tt.args.parse); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewFromBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}
