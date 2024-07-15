package orm

import (
	"github.com/Seann-Moser/cutil/sqlc/orm/db"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

// Test cases for Query struct
func TestQuery_Select(t *testing.T) {
	q := &Query[Role]{}
	q.Select(db.Column{Name: "col1"}, db.Column{Name: "col2"})
	assert.Equal(t, 2, len(q.SelectColumns))
	assert.Equal(t, "col1", q.SelectColumns[0].Name)
	assert.Equal(t, "col2", q.SelectColumns[1].Name)
}

func TestQuery_SkipCache(t *testing.T) {
	q := &Query[Role]{}
	q.SkipCache()
	assert.True(t, q.skipCache)
}

func TestQuery_From(t *testing.T) {
	q := &Query[Role]{}
	fromQuery := &Query[Role]{}
	q.From(fromQuery)
	assert.Equal(t, fromQuery, q.FromQuery)
}

//func TestQuery_Column(t *testing.T) {
//	table := &Table[Role]{
//		Columns: []db.Column{
//			{Name: "col1"},
//		},
//	}
//	q := &Query[Role]{FromTable: table}
//	col := q.Column("col1")
//	assert.Equal(t, "col1", col.Name)
//}

func TestQuery_Join(t *testing.T) {
	q := &Query[Role]{}
	q.Join(map[string]db.Column{"col1": {Name: "col1"}}, "INNER JOIN")
	assert.Equal(t, 1, len(q.JoinStmt))
	assert.Equal(t, "INNER JOIN", q.JoinStmt[0].JoinType)
}

func TestQuery_MapColumns(t *testing.T) {
	q := &Query[Role]{}
	q.MapColumns(db.Column{Name: "col1"})
	assert.Equal(t, 1, len(q.MapKeyColumns))
	assert.Equal(t, "col1", q.MapKeyColumns[0].Name)
}

func TestQuery_UniqueWhere(t *testing.T) {
	q := &Query[Role]{}
	q.UniqueWhere(db.Column{Name: "col1"}, "=", "AND", 0, "value", false)
	assert.Equal(t, 1, len(q.WhereStmts))
	assert.Equal(t, "col1", q.WhereStmts[0].LeftValue.Name)
}

func TestQuery_Where(t *testing.T) {
	q := &Query[Role]{}
	q.Where(db.Column{Name: "col1"}, "=", "AND", 0, "value")
	assert.Equal(t, 1, len(q.WhereStmts))
	assert.Equal(t, "col1", q.WhereStmts[0].LeftValue.Name)
}

func TestQuery_GroupBy(t *testing.T) {
	q := &Query[Role]{}
	q.GroupBy(db.Column{Name: "col1"})
	assert.Equal(t, 1, len(q.GroupByStmt))
	assert.Equal(t, "col1", q.GroupByStmt[0].Name)
}

func TestQuery_OrderBy(t *testing.T) {
	q := &Query[Role]{}
	q.OrderBy(db.Column{Name: "col1"})
	assert.Equal(t, 1, len(q.OrderByStmt))
	assert.Equal(t, "col1", q.OrderByStmt[0].Name)
}

func TestQuery_SetCacheDuration(t *testing.T) {
	q := &Query[Role]{}
	q.SetCacheDuration(10 * time.Minute)
	assert.Equal(t, 10*time.Minute, q.CacheDuration)
}

func TestQuery_Limit(t *testing.T) {
	q := &Query[Role]{}
	q.Limit(10)
	assert.Equal(t, 10, q.LimitCount)
}

func TestQuery_UseCache(t *testing.T) {
	q := &Query[Role]{}
	q.UseCache()
	assert.True(t, q.useCache)
}

//func TestQuery_Run(t *testing.T) {
//	mockDB := new(db.MockDB)
//	q := &Query[Role]{
//		FromTable: &Table[Role]{
//			Name: "test_table",
//			db:   mockDB,
//			Columns: map[string]db.Column{
//				"col1": {Name: "col1"},
//			},
//		},
//	}
//
//	q.Build()
//
//	_, err := q.Run(context.Background(), mockDB)
//	assert.NoError(t, err)
//}
//
//func TestQuery_RunSingle(t *testing.T) {
//	mockDB := new(db.MockDB)
//	q := &Query[Role]{
//		FromTable: &Table[Role]{
//			Name: "test_table",
//			db:   mockDB,
//		},
//	}
//	ctx := context.Background()
//
//	//mockDB.On("NamedQuery", ctx, mock.Anything, mock.Anything).Return(&sql.Rows{}, nil)
//
//	_, err := q.RunSingle(ctx, mockDB)
//	assert.NoError(t, err)
//}
//
//func TestQuery_RunMap(t *testing.T) {
//	mockDB := new(db.MockDB)
//	q := &Query[Role]{
//		FromTable: &Table[Role]{
//			Name: "test_table",
//			db:   mockDB,
//		},
//		MapKeyColumns: []db.Column{{Name: "col1"}},
//	}
//	ctx := context.Background()
//
//	//mockDB.On("NamedQuery", ctx, mock.Anything, mock.Anything).Return(&sql.Rows{}, nil)
//
//	_, err := q.RunMap(ctx, mockDB)
//	assert.NoError(t, err)
//}
