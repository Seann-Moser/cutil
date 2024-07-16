package db

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"time"
)

type MockDB struct {
	Tables   map[string]*MockTable
	MockData map[string]map[string]*MockData
	Prefix   string
}

func (m *MockDB) GetDataset(ds string) string {
	if len(m.Prefix) > 0 {
		return m.Prefix + ds
	}
	return ds
}

type MockTable struct {
	name    string
	dataset string
	columns map[string]Column
}

type MockData struct {
	Name    string
	Dataset string
	Columns map[string]Column
}

func NewMockDB() *MockDB {
	return &MockDB{
		Tables: map[string]*MockTable{},
	}
}

func (m *MockDB) Ping(ctx context.Context) error {
	return nil
}

func (m *MockDB) CreateTable(ctx context.Context, dataset, table string, columns map[string]Column) error {
	m.Tables[fmt.Sprintf("%s.%s", dataset, table)] = &MockTable{
		name:    table,
		dataset: dataset,
		columns: columns,
	}
	for _, col := range columns {
		if !isValidColumnName(col.Name) {
			return fmt.Errorf("column Name %s is not valid", col.Name)
		}
	}
	return nil
}

func (m *MockDB) QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error) {
	if valid, err := isSQLValid(query); err != nil && !valid {
		return nil, fmt.Errorf("invalid query %s: %v", query, err)
	}
	return nil, nil
}

func (m *MockDB) ExecContext(ctx context.Context, query string, args interface{}) error {
	query = replacePlaceholdersWithRandomValues(query)
	if valid, err := isSQLValid(query); err != nil && !valid {
		return fmt.Errorf("invalid query %s: %v", query, err)
	}
	return nil
}

func (m *MockDB) Close() {
}

var _ DB = &MockDB{}

func generateRandomValue() string {
	return fmt.Sprintf("%d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1000))
}

// Function to replace all :{[0-9]+_*\w+} with a random value in a string
func replacePlaceholdersWithRandomValues(input string) string {
	// Compile the regex pattern
	re := regexp.MustCompile(`:\d+_*\w+`)

	// Replace each placeholder with a random value
	result := re.ReplaceAllStringFunc(input, func(_ string) string {
		return generateRandomValue()
	})

	return result
}
