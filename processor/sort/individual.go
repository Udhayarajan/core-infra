package sort

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"core-infra/common"
)

var sortFuncMapper = map[string]func(a, b *common.CSV) bool{
	"sorted_id":        byID,
	"sorted_name":      byName,
	"sorted_continent": byContinent,
}

func IndividualFileSort(runID string) error {
	rootPath := filepath.Join("csv", runID)
	dir, err := os.ReadDir(rootPath)
	if err != nil {
		return err
	}

	for _, entry := range dir {
		if entry.IsDir() {
			continue
		}

		if err := readAndSortFile(rootPath, entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

func readAndSortFile(rootPath, fileName string) error {
	file, err := os.Open(filepath.Join(rootPath, fileName))
	if err != nil {
		return err
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("failed to close file", slog.Any("err", err), slog.String("file", fileName))
		}
	}()

	scanner := bufio.NewScanner(file)

	var csvData []*common.CSV
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		csvData = append(csvData, common.NewFromBytes(line, true))
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	for parentPath, fn := range sortFuncMapper {
		sortPath := filepath.Join(rootPath, parentPath, fileName)
		if err := sortAndSave(sortPath, csvData, fn); err != nil {
			return err
		}
	}

	return nil
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
