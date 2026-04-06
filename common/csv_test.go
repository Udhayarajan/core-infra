package common

import (
	"bytes"
	"encoding/csv"
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
