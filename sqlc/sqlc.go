package sqlc

import (
	"context"
	"fmt"
	"github.com/Seann-Moser/cutil/sqlc/orm"
	"github.com/Seann-Moser/cutil/sqlc/orm/db"
	"strings"
)

func GetQuery[T any](ctx context.Context) *orm.Query[T] {
	table, err := GetTableCtx[T](ctx)
	if err != nil {
		return &orm.Query[T]{Err: err}
	}
	q := orm.QueryTable[T](table)
	return q
}

func GetTableCtx[T any](ctx context.Context, suffix ...string) (*orm.Table[T], error) {
	var s T
	name := orm.ToSnakeCase(getType(s))

	value := ctx.Value(orm.TableCtxName(strings.Join(append([]string{name}, suffix...), "_")))
	if value == nil {
		return nil, orm.ErrTableNotInCtx
	}
	return value.(*orm.Table[T]), nil
}

func InsertCtx[T any](ctx context.Context, data *T, suffix ...string) (string, error) {
	table, err := GetTableCtx[T](ctx, suffix...)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", fmt.Errorf("no data provided")
	}
	id, err := table.Insert(ctx, nil, *data)
	if err != nil {
		return "", err
	}
	return id, nil
}

func DeleteAllCtx[T any](ctx context.Context, data []*T, suffix ...string) error {
	table, err := GetTableCtx[T](ctx, suffix...)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("no data provided")
	}
	for _, d := range data {
		err = table.Delete(ctx, nil, *d)
		if err != nil {
			return err
		}
	}
	return nil
}

func DeleteCtx[T any](ctx context.Context, data *T, suffix ...string) error {
	table, err := GetTableCtx[T](ctx, suffix...)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("no data provided")
	}
	return table.Delete(ctx, nil, *data)
}

func UpdateCtx[T any](ctx context.Context, data *T, suffix ...string) error {
	table, err := GetTableCtx[T](ctx, suffix...)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("no data provided")
	}
	return table.Update(ctx, nil, *data)
}

func ListCtx[T any](ctx context.Context, stmt ...*orm.WhereStmt) ([]*T, error) {
	q := GetQuery[T](ctx)
	q.WhereStmts = append(q.WhereStmts, stmt...)
	return q.Run(ctx, nil)
}

func GetIDCtx[T any](ctx context.Context, id string) (*T, error) {
	q := GetQuery[T](ctx)
	q.Where(q.Column("id"), "=", "AND", 0, id)
	return q.RunSingle(ctx, nil)
}

func GetColumn[T any](ctx context.Context, name string) db.Column {
	return GetQuery[T](ctx).Column(name)
}
