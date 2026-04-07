package sort

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"core-infra/common"

	"golang.org/x/sync/errgroup"
)

var sortFuncMapper = map[string]func(a, b *common.CSV) bool{
	"sorted_id":        byID,
	"sorted_name":      byName,
	"sorted_continent": byContinent,
}

type Sorter struct {
	runID         string
	readSizeLimit int64
	rootPath      string // csv/{runID}
}

func NewSorter(runID string, readCountLimit int64) *Sorter {
	return &Sorter{
		runID:         runID,
		readSizeLimit: readCountLimit,
		rootPath:      filepath.Join("csv", runID),
	}
}

func (s *Sorter) IndividualFileSort() error {
	slog.Info("starting individual file sort")
	dir, err := os.ReadDir(s.rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range dir {
		if entry.IsDir() {
			continue
		}

		if err := s.readAndSortFile(entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sorter) readAndSortFile(fileName string) error {
	file, err := os.Open(filepath.Join(s.rootPath, fileName))
	if err != nil {
		return err
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("failed to close file", slog.Any("err", err), slog.String("file", fileName))
		}
	}()

	scanner := bufio.NewScanner(file)

	csvData := make([]*common.CSV, 0, s.readSizeLimit)
	currentLen := int64(0)
	batchCount := 0
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		csvData = append(csvData, common.NewFromBytes(line, true))
		currentLen += int64(len(line))
		if currentLen >= s.readSizeLimit {
			if err := scanner.Err(); err != nil {
				return err
			}
			batchFileName := fmt.Sprintf("%d_%s", batchCount, fileName)
			if err := s.batchSorter(batchFileName, csvData); err != nil {
				return err
			}
			csvData = make([]*common.CSV, 0, s.readSizeLimit)
			currentLen = 0
		}
	}

	return nil
}

func (s *Sorter) batchSorter(fileName string, csvData []*common.CSV) error {

	g := errgroup.Group{}

	for parentPath, fn := range sortFuncMapper {
		sortPath := filepath.Join(s.rootPath, parentPath, fileName)
		g.Go(func() error {
			return sortAndSave(sortPath, csvData, fn)
		})
	}

	return g.Wait()
}

func write(name string, data []*common.CSV) error {
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil && !os.IsExist(err) {
		return err
	}

	file, err := os.Create(name)
	if err != nil {
		return err
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("failed to close file", slog.Any("err", err), slog.String("file", name))
		}
	}()

	for _, csvData := range data {
		if _, err := file.Write(append(csvData.Bytes(), '\n')); err != nil {
			return err
		}
	}

	return nil
}

func sortAndSave(name string, csvData []*common.CSV, by func(a *common.CSV, b *common.CSV) bool) error {
	copyData := make([]*common.CSV, len(csvData))
	copy(copyData, csvData)
	sort.Slice(copyData, func(i, j int) bool {
		return by(copyData[i], copyData[j])
	})

	if err := write(name, copyData); err != nil {
		return err
	}

	return nil
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
