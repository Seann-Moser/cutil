package orm

import (
	"context"
	"testing"

	"github.com/Seann-Moser/cutil/sqlc/orm/db"
	"github.com/stretchr/testify/assert"
)

func TestTable_InitializeTable(t *testing.T) {
	mockDB := db.NewMockDB()
	table, err := NewTable[Role]("test_dataset", QueryTypeSQL)
	assert.NoError(t, err)

	err = table.InitializeTable(context.Background(), mockDB)
	assert.NoError(t, err)
	assert.Equal(t, "test_dataset", table.Dataset)
	assert.Equal(t, "role", table.Name)
}

func TestTable_GetPrimary(t *testing.T) {
	table, err := NewTable[Role]("test_dataset", QueryTypeSQL)
	assert.NoError(t, err)

	primaryColumns := table.GetPrimary()
	assert.Equal(t, 1, len(primaryColumns))
	assert.Equal(t, "id", primaryColumns[0].Name)
}

func TestTable_GetColumn(t *testing.T) {
	table, err := NewTable[Role]("test_dataset", QueryTypeSQL)
	assert.NoError(t, err)

	column := table.GetColumn("name")
	assert.Equal(t, "name", column.Name)
}

func TestTable_Insert(t *testing.T) {
	mockDB := db.NewMockDB()
	table, err := NewTable[Role]("test_dataset", QueryTypeSQL)
	assert.NoError(t, err)

	err = table.InitializeTable(context.Background(), mockDB)
	assert.NoError(t, err)

	role := Role{
		ID:          "1",
		Name:        "Admin",
		Description: "Administrator role",
		Public:      true,
		Priority:    1,
	}

	id, err := table.Insert(context.Background(), mockDB, role)
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
}

func TestTable_Upsert(t *testing.T) {
	mockDB := db.NewMockDB()
	table, err := NewTable[Role]("test_dataset", QueryTypeSQL)
	assert.NoError(t, err)

	err = table.InitializeTable(context.Background(), mockDB)
	assert.NoError(t, err)

	role := Role{
		ID:          "1",
		Name:        "Admin",
		Description: "Administrator role",
		Public:      true,
		Priority:    1,
	}

	id, err := table.Upsert(context.Background(), mockDB, role)
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
}

func TestTable_Delete(t *testing.T) {
	mockDB := db.NewMockDB()
	table, err := NewTable[Role]("test_dataset", QueryTypeSQL)
	assert.NoError(t, err)

	err = table.InitializeTable(context.Background(), mockDB)
	assert.NoError(t, err)

	role := Role{
		ID: "1",
	}

	err = table.Delete(context.Background(), mockDB, role)
	assert.NoError(t, err)
}

func TestTable_Update(t *testing.T) {
	mockDB := db.NewMockDB()
	table, err := NewTable[Role]("test_dataset", QueryTypeSQL)
	assert.NoError(t, err)

	err = table.InitializeTable(context.Background(), mockDB)
	assert.NoError(t, err)

	role := Role{
		ID:          "1",
		Name:        "Admin",
		Description: "Administrator role",
		Public:      true,
		Priority:    1,
	}

	err = table.Update(context.Background(), mockDB, role)
	assert.NoError(t, err)
}
