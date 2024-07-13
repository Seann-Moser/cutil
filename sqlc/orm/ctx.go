package orm

import (
	"context"
	"errors"
	"github.com/Seann-Moser/cutil/sqlc/orm/db"
)

var (
	ErrTableNotInCtx = errors.New("table is missing from context")
	ErrDBNotInCtx    = errors.New("db is missing from context")
)

type TableCtxName string
type DBCtxName string

const DBContext = "db-base-context"

func AddTableCtx[T any](ctx context.Context, db db.DB, dataset string, queryType QueryType, suffix ...string) (context.Context, error) {
	table, err := NewTable[T](dataset, queryType)
	if err != nil {
		return ctx, err
	}
	if _, err := GetDBContext(ctx, ""); err != nil {
		AddDBContext(ctx, DBContext, db)
	}

	err = table.InitializeTable(ctx, db, suffix...)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, TableCtxName(table.Name), table)
	return ctx, nil
}

func AddDBContext(ctx context.Context, name string, db db.DB) context.Context {
	if name == "" {
		name = DBContext
	}
	ctx = context.WithValue(ctx, DBCtxName(name), db)
	return ctx
}

func GetDBContext(ctx context.Context, name string) (db.DB, error) {
	if name == "" {
		name = DBContext
	}
	value := ctx.Value(DBCtxName(name))
	if value == nil {
		return nil, ErrDBNotInCtx
	}
	return value.(db.DB), nil
}

func WithTableContext(baseCtx context.Context, tableCtx context.Context, names ...string) (context.Context, error) {
	for _, name := range names {
		value := tableCtx.Value(TableCtxName(name))
		if value == nil {
			return nil, ErrTableNotInCtx
		}
		baseCtx = context.WithValue(baseCtx, TableCtxName(name), value)

	}
	return baseCtx, nil
}
