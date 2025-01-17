package orm

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/Seann-Moser/cutil/sqlc/orm/db"
	"reflect"
	"strings"
	"time"

	"github.com/Seann-Moser/cutil/cachec"
	"go.opentelemetry.io/otel"
)

type Query[T any] struct {
	Name                  string
	Err                   error
	SelectColumns         []db.Column
	DistinctSelectColumns []db.Column
	FromTable             *Table[T]
	FromQuery             *Query[T]
	JoinStmt              []*JoinStmt
	WhereStmts            []*WhereStmt
	GroupByStmt           []db.Column
	OrderByStmt           []db.Column
	MapKeyColumns         []db.Column
	LimitCount            int

	Cache         cachec.Cache
	useCache      bool
	Query         string
	skipCache     bool
	CacheDuration time.Duration
	WhereColumns  map[string]int

	Pagination struct {
		Limit              int
		Offset             int
		PreviousPageColumn db.Column
		PreviewColumnValue interface{}
	}
}

type JoinStmt struct {
	Columns  map[string]db.Column
	JoinType string
}

func generateGroupBy(groupBy []db.Column) string {
	var columns []string
	for _, c := range groupBy {
		if c.Name == "" {
			return ""
		}
		if c.SelectAs == "" {
			if c.GroupByName != "" {
				columns = append(columns, c.GroupByName)
			} else {
				columns = append(columns, c.Name)
			}
		} else {
			columns = append(columns, c.SelectAs)
		}

	}
	return "GROUP BY " + strings.Join(columns, ",")
}

func QueryTable[T any](table *Table[T]) *Query[T] {
	return &Query[T]{
		Name:                  "",
		Err:                   nil,
		SelectColumns:         []db.Column{},
		DistinctSelectColumns: []db.Column{},
		FromTable:             table,
		FromQuery:             nil,
		JoinStmt:              make([]*JoinStmt, 0),
		WhereStmts:            make([]*WhereStmt, 0),
		GroupByStmt:           make([]db.Column, 0),
		OrderByStmt:           make([]db.Column, 0),
		MapKeyColumns:         make([]db.Column, 0),
		WhereColumns:          map[string]int{},
		CacheDuration:         0,
		LimitCount:            0,
		Cache:                 nil,
		Query:                 "",
		skipCache:             false,
	}
}

func (q *Query[T]) Select(columns ...db.Column) *Query[T] {
	for _, c := range columns {
		if c.Name == "" {
			continue
		}
		q.SelectColumns = append(q.SelectColumns, c)
	}
	return q
}

func (q *Query[T]) SkipCache() *Query[T] {
	q.skipCache = true
	return q
}

func (q *Query[T]) From(query *Query[T]) *Query[T] {
	q.FromQuery = query
	return q
}

func (q *Query[T]) Column(name string) db.Column {
	if q.Err != nil {
		return db.Column{}
	}
	c := q.FromTable.GetColumn(name)
	if c.Name == "" {
		q.Err = fmt.Errorf("missing column from table(%s) %s", q.FromTable.FullTableName(), name)
	}
	return c
}

func (q *Query[T]) Join(tableColumns map[string]db.Column, joinType string) *Query[T] {
	q.JoinStmt = append(q.JoinStmt, &JoinStmt{
		Columns:  tableColumns,
		JoinType: joinType,
	})
	return q
}

func (q *Query[T]) JoinColumn(joinType string, tableColumns db.Column) *Query[T] {
	q.JoinStmt = append(q.JoinStmt, &JoinStmt{
		Columns:  map[string]db.Column{tableColumns.Name: tableColumns},
		JoinType: joinType,
	})
	return q
}

func (q *Query[T]) MapColumns(column ...db.Column) *Query[T] {
	if column == nil {
		return q
	}
	q.MapKeyColumns = append(q.MapKeyColumns, column...)
	return q
}
func (q *Query[T]) UniqueWhere(column db.Column, conditional, joinOperator string, level int, value interface{}, flip bool) *Query[T] {
	if level < 0 {
		level = 0
	}
	if q.WhereColumns == nil {
		q.WhereColumns = map[string]int{}
	}
	if column.Name == "" {
		return q
	}
	stmt := &WhereStmt{
		LeftValue:    column,
		Conditional:  conditional,
		RightValue:   value,
		Level:        level,
		JoinOperator: joinOperator,
		Flip:         flip,
	}
	if _, found := q.WhereColumns[column.FullTableName()]; !found {
		q.WhereColumns[column.FullTableName()] = 0
	} else {
		q.WhereColumns[column.FullTableName()]++
	}
	stmt.Index = q.WhereColumns[column.FullTableName()]
	q.WhereStmts = append(q.WhereStmts, stmt)
	return q
}

func (q *Query[T]) Where(column db.Column, conditional, joinOperator string, level int, value interface{}) *Query[T] {
	if level < 0 {
		level = 0
	}
	if column.Name == "" {
		return q
	}
	q.WhereStmts = append(q.WhereStmts, &WhereStmt{
		LeftValue:    column,
		Conditional:  conditional,
		RightValue:   value,
		Level:        level,
		JoinOperator: joinOperator,
	})

	return q
}

func (q *Query[T]) W(column db.Column, conditional string, value interface{}) *Query[T] {
	if column.Name == "" {
		return q
	}
	q.WhereStmts = append(q.WhereStmts, &WhereStmt{
		LeftValue:    column,
		Conditional:  conditional,
		RightValue:   value,
		Level:        0,
		JoinOperator: "AMD",
	})
	return q
}

func (q *Query[T]) GroupBy(column ...db.Column) *Query[T] {
	for _, c := range column {
		if c.Name == "" {
			continue
		}
		if len(q.SelectColumns) > 0 {
			for _, selectColumn := range q.SelectColumns {
				if strings.EqualFold(c.Name, selectColumn.Name) {
					q.GroupByStmt = append(q.GroupByStmt, selectColumn)
					break
				}
			}
		} else {
			q.GroupByStmt = append(q.GroupByStmt, c)
		}

	}
	return q
}

func (q *Query[T]) OrderBy(column ...db.Column) *Query[T] {
	for _, c := range column {
		if c.Name == "" {
			continue
		}
		q.OrderByStmt = append(q.OrderByStmt, c)
	}
	return q
}

func (q *Query[T]) SetCacheDuration(duration time.Duration) *Query[T] {
	q.CacheDuration = duration
	return q
}

func (q *Query[T]) Limit(limit int) *Query[T] {
	q.LimitCount = limit
	return q
}

func (q *Query[T]) SetCache(cache cachec.Cache) *Query[T] {
	q.Cache = cache
	return q
}
func (q *Query[T]) UseCache() *Query[T] {
	q.useCache = true
	return q
}

func (q *Query[T]) Build() *Query[T] {
	switch q.FromTable.QueryType {
	case QueryTypeFireBase:
	case QueryTypeSQL:
		fallthrough
	default:
		query := q.buildSqlQuery()
		return query
	}
	return q
}
func (q *Query[T]) SetName(name string) *Query[T] {
	q.Name = name
	return q
}

func (q *Query[T]) getName() string {
	if len(q.Name) != 0 {
		return q.Name
	}
	args := []string{
		q.FromTable.Name,
	}
	if len(q.WhereStmts) > 0 {
		args = append(args, "where")
	}
	for _, w := range q.WhereStmts {
		args = append(args, w.LeftValue.Name)
	}
	if len(q.GroupByStmt) > 0 {
		args = append(args, "group by")
	}
	for _, w := range q.GroupByStmt {
		args = append(args, w.Name)
	}

	return strings.ToLower(strings.Join(args, "_"))
}

func (q *Query[T]) RunMap(ctx context.Context, db db.DB, args ...interface{}) (map[string]*T, error) {
	rows, err := q.Run(ctx, db, args)
	if err != nil {
		return nil, err
	}
	if len(q.MapKeyColumns) == 0 {
		q.MapKeyColumns = append(q.MapKeyColumns, q.FromTable.GetPrimary()...)
	}
	m := map[string]*T{}

	for _, row := range rows {
		// pointer to struct - addressable
		ps := reflect.ValueOf(row)
		// struct
		s := ps.Elem()
		for _, column := range q.MapKeyColumns {
			if s.Kind() == reflect.Struct {
				f := s.FieldByName(column.Name)
				if f.IsValid() {
					m[f.String()] = row
				}
			}
		}
	}
	return m, err
}

func (q *Query[T]) RunSingle(ctx context.Context, db db.DB, args ...interface{}) (*T, error) {
	rows, err := q.Limit(1).Run(ctx, db, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, sql.ErrNoRows
	}
	return rows[0], nil
}

func (q *Query[T]) RunCtx(ctx context.Context) ([]*T, error) {
	return q.Run(ctx, nil)
}

func (q *Query[T]) Run(ctx context.Context, db db.DB, args ...interface{}) ([]*T, error) {
	if q.Err != nil {
		return nil, q.Err
	}
	if q.Name != "" {
		query, err := cachec.Get[string](ctx, "queries", q.Name)
		if err == nil && *query != "" {
			q.Query = *query
		}
	}
	if len(q.Query) == 0 {
		q.Build()
	}
	if q.Name != "" {
		_ = cachec.SetWithExpiration[string](ctx, 30*time.Minute, "queries", q.Name, q.Query)
	}
	ctx = CtxWithQueryTag(ctx, q.getName())
	cacheKey := q.GetCacheKey(args...)

	if q.Cache != nil {
		data, err := cachec.GetFromCache[[]*T](ctx, q.Cache, q.FromTable.FullTableName(), cacheKey)
		if err == nil && len(*data) > 0 {
			return *data, nil
		}
	}
	if q.useCache {
		tracer := otel.GetTracerProvider()
		ctx, span := tracer.Tracer("query-ctx").Start(ctx, fmt.Sprintf("%s-%s", q.Name, q.FromTable.FullTableName()))
		defer span.End()
		return cachec.GetSet[[]*T](ctx, q.CacheDuration, q.FromTable.FullTableName(), cacheKey, func(ctx context.Context) ([]*T, error) {
			return q.FromTable.NamedSelect(ctx, db, q.Query, q.Args(args))
		})
	}
	tracer := otel.GetTracerProvider()
	ctx, span := tracer.Tracer("query").Start(ctx, fmt.Sprintf("%s-%s", q.Name, q.FromTable.FullTableName()))
	defer span.End()
	data, err := q.FromTable.NamedSelect(ctx, db, q.Query, q.Args(args))
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if q.Cache != nil {
		_ = cachec.SetFromCache[[]*T](ctx, q.Cache, q.FromTable.FullTableName(), cacheKey, data)
	}
	return data, nil
}

func (q *Query[T]) Args(args ...interface{}) map[string]interface{} {
	whereArgs := map[string]interface{}{}
	for _, where := range q.WhereStmts {
		if k, arg := where.GetArg(); arg != nil || k != "" {
			whereArgs[k] = arg
		}
	}
	arg, err := combineStructs(append(args, whereArgs)...)
	if err != nil {
		return nil
	}
	return arg
}

func (q *Query[T]) GetCacheKey(args ...interface{}) string {
	var keys []string
	argsData := q.Args(args...)

	keys = append(keys, q.FromTable.FullTableName())

	for _, k := range q.SelectColumns {
		keys = append(keys, k.Name)
	}
	for _, k := range q.WhereStmts {
		keys = append(keys, k.ToString())
	}
	for _, k := range q.GroupByStmt {
		keys = append(keys, k.FullTableName())
	}
	for _, k := range q.OrderByStmt {
		keys = append(keys, k.FullTableName())
	}

	for k, v := range argsData {
		keys = append(keys, fmt.Sprintf("%s:%s", k, safeString(v)))
	}

	return GetMD5Hash(strings.Join(keys, ""))
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func (q *Query[T]) buildSqlQuery() *Query[T] {
	if q.Err != nil {
		return q
	}
	var isGroupBy = len(q.GroupByStmt) > 0
	var query string
	selectColumns := q.FromTable.GetSelectableColumns(isGroupBy, q.SelectColumns...)

	if q.FromQuery != nil {
		q.FromQuery.Build()
		query = fmt.Sprintf("SELECT\n\t%s\nFROM\n\t(%s)", strings.Join(selectColumns, ",\n\t"), strings.ReplaceAll(q.FromQuery.Query, "\n", "\n\t"))

	} else {
		query = fmt.Sprintf("SELECT\n\t%s\nFROM\n\t%s", strings.Join(selectColumns, ",\n\t"), q.FromTable.FullTableName())

	}

	if len(q.JoinStmt) > 0 {
		for _, join := range q.JoinStmt {
			overlappingColumns := map[string]db.Column{}
			overlappingColumns = JoinMaps[db.Column](overlappingColumns, q.FromTable.GetCommonColumns(join.Columns))
			if len(overlappingColumns) == 0 {
				continue
			}
			query = fmt.Sprintf("%s\n%s", query, q.FromTable.generateJoinStmt(overlappingColumns, join.JoinType))
		}
	}

	if len(q.WhereStmts) > 0 {
		query = fmt.Sprintf("%s\n%s", query, generateWhere(q.WhereStmts))
	}

	if len(q.GroupByStmt) > 0 {
		query = fmt.Sprintf("%s\n%s", query, generateGroupBy(q.GroupByStmt))
	}

	if len(q.OrderByStmt) > 0 {
		query = fmt.Sprintf("%s\n%s", query, q.FromTable.OrderByColumns(len(q.GroupByStmt) > 0, q.OrderByStmt...))
	}

	if q.LimitCount > 0 {
		query = fmt.Sprintf("%s\nLIMIT %d;", query, q.LimitCount)
	}
	q.Query = query
	return q
}

func SelectQuery[T any, X any](ctx context.Context, db db.DB, q *Query[T], args ...interface{}) ([]*X, error) {
	if len(q.Query) == 0 {
		q.Build()
	}
	if db == nil {
		db = q.FromTable.db
	}
	rows, err := NamedQuery(ctx, db, q.Query, q.Args(args...))
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, sql.ErrNoRows
	}
	var output []*X
	for rows.Next() {
		var tmp X
		err := rows.StructScan(&tmp)
		if err != nil {
			return nil, err
		}
		output = append(output, &tmp)
	}
	return output, nil
}

func Select[T any](ctx context.Context, db db.DB, q string, args ...interface{}) ([]*T, error) {
	if db == nil {
		var err error
		db, err = GetDBContext(ctx, "")
		if err != nil {
			return nil, fmt.Errorf("failed to get db context: %v", err)
		}
	}
	rows, err := NamedQuery(ctx, db, q, args)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, sql.ErrNoRows
	}
	var output []*T
	for rows.Next() {
		var tmp T
		err := rows.StructScan(&tmp)
		if err != nil {
			return nil, err
		}
		output = append(output, &tmp)
	}
	return output, nil
}
