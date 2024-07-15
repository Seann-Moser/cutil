package sqlc

import (
	"context"
	"github.com/Seann-Moser/cutil/sqlc/orm"
	"github.com/Seann-Moser/cutil/sqlc/orm/db"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Role struct {
	ID          string `json:"id" db:"id" qc:"primary;join;join_name::role_id;auto_generate_id"`
	Name        string `json:"name" db:"name" qc:"update;data_type::varchar(512);"`
	Description string `json:"description" db:"description" qc:"data_type::varchar(512);update"`
	Public      bool   `json:"public" db:"public" qc:"default::false;update"`
	Priority    int    `json:"priority" db:"priority" qc:"default::0;update"`

	UpdatedTimestamp string `json:"updated_timestamp" db:"updated_timestamp" qc:"skip;default::updated_timestamp"`
	CreatedTimestamp string `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp"`
}

func TestAddTable(t *testing.T) {
	mockORM := db.NewMockDB()

	dao := &DAO{
		db:           mockORM,
		tablesNames:  []string{},
		tableColumns: map[string]map[string]db.Column{},
	}

	dao.SetContext(orm.AddDBContext(context.Background(), "", mockORM))

	err := AddTable[Role](dao, "dataset", orm.QueryTypeSQL)
	assert.NoError(t, err)
	assert.Contains(t, dao.tablesNames, "role")
}
