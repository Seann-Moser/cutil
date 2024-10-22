package orm

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/Seann-Moser/cutil/sqlc/orm/db"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/Seann-Moser/cutil/cachec"
	"go.opentelemetry.io/otel"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type QueryType string

const (
	QueryTypeSQL      = "sql"
	QueryTypeFireBase = "firebase"
)
const (
	TagConfigPrefix     = "qc"
	TagColumnNamePrefix = "db"
)

// todo clear cache for tables when crud operation happens
var (
	NoOverlappingColumnsErr = errors.New("error: no overlapping columns found")
	matchFirstCap           = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap             = regexp.MustCompile("([a-z0-9])([A-Z])")
)

type Table[T any] struct {
	Dataset   string               `json:"dataset"`
	Name      string               `json:"name"`
	Columns   map[string]db.Column `json:"columns"`
	QueryType QueryType            `json:"query_type"`
	db        db.DB
}

func NewTable[T any](databaseName string, queryType QueryType) (*Table[T], error) {
	var err error
	var s T
	newTable := Table[T]{
		Dataset:   databaseName,
		Name:      ToSnakeCase(getType(s)),
		Columns:   map[string]db.Column{},
		QueryType: queryType,
	}

	structType := reflect.TypeOf(s)
	var setPrimary bool
	for i := 0; i < structType.NumField(); i++ {
		var column *db.Column
		field := structType.Field(i)
		name := field.Tag.Get(TagColumnNamePrefix)
		if name == "" {
			name = structType.Field(i).Name
		}

		if value := field.Tag.Get(TagConfigPrefix); value != "" {
			column, err = GetColumnFromTag(name, value, field.Type)
		} else {
			column, err = GetColumnFromTag(name, "", field.Type)
		}

		if err != nil {
			return nil, fmt.Errorf("failed parsing struct tag info(%s):%w",
				field.Tag.Get(TagConfigPrefix),
				err)
		}
		column.ColumnOrder = i
		column.Table = newTable.Name
		column.Dataset = databaseName

		if column.Primary {
			setPrimary = true
		}
		if column.Name == "-" {
			continue
		}
		newTable.Columns[column.Name] = *column
	}
	if !setPrimary {
		return nil, db.MissingPrimaryKeyErr
	}
	return &newTable, nil
}

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func (t *Table[T]) GetDB() db.DB {
	return t.db
}

func (t *Table[T]) GetPrimary() []db.Column {
	var primaryColumns []db.Column
	for _, c := range t.GetColumns() {
		if c.Primary {
			primaryColumns = append(primaryColumns, c)
		}

	}
	return primaryColumns
}

func (t *Table[T]) GetColumn(name string) db.Column {
	if column, found := t.GetColumns()[ToSnakeCase(name)]; found {
		return column
	}
	return db.Column{}
}

func (t *Table[T]) InitializeTable(ctx context.Context, d db.DB, suffix ...string) error {
	if t.db == nil {
		t.db = d
	}
	if d == nil {
		return fmt.Errorf("no db set")
	}
	t.Name = strings.Join(append([]string{t.Name}, suffix...), "_")
	err := d.CreateTable(ctx, d.GetDataset(t.Dataset), t.Name, t.Columns)
	if err != nil {
		return err
	}
	return nil
}

func (t *Table[T]) GetColumns() map[string]db.Column {
	c := map[string]db.Column{}
	if t.db == nil {
		return t.Columns
	}
	for k, v := range t.Columns {
		ts := v
		ts.Dataset = t.db.GetDataset(t.Dataset)
		c[k] = ts
	}
	return c
}

func (t *Table[T]) FullTableName() string {
	if t.db == nil {
		return fmt.Sprintf("%s.%s", t.Dataset, t.Name)
	}
	return fmt.Sprintf("%s.%s", t.db.GetDataset(t.Dataset), t.Name)
}

func (t *Table[T]) WhereValues(whereElementsStr ...string) []string {
	var whereValues []string
	for _, i := range whereElementsStr {
		column, found := t.Columns[i]
		if !found {
			continue
		}

		tmp := column.Where
		if column.Where == "" {
			tmp = "="
		}
		var formatted string
		switch strings.TrimSpace(strings.ToLower(tmp)) {
		case "not in":
			fallthrough
		case "in":
			formatted = fmt.Sprintf("%s %s (:%s)", column.FullName(false, false), tmp, column.Name)
		default:
			formatted = fmt.Sprintf("%s %s :%s", column.FullName(false, false), tmp, column.Name)
		}
		if strings.Contains(formatted, ".") {
			whereValues = append(whereValues, formatted)
		}

	}
	return whereValues

}

func (t *Table[T]) Select(ctx context.Context, d db.DB, conditional string, groupBy bool, args ...interface{}) ([]*T, error) {
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(t.GetSelectableColumns(groupBy), ","), t.FullTableName())
	keys, err := getKeys(args...)
	if err != nil {
		return nil, err
	}

	if len(keys) > 0 {
		query = fmt.Sprintf("%s %s", query, t.WhereStatement(strings.ToUpper(conditional), keys...))
	}

	order := t.OrderByStatement(false)
	if len(order) > 0 {
		query = fmt.Sprintf("%s %s", query, order)
	}
	return t.NamedSelect(ctx, d, query, args...)
}

func (t *Table[T]) NamedSelect(ctx context.Context, d db.DB, query string, args ...interface{}) ([]*T, error) {
	if d == nil {
		d = t.db
	}
	if d == nil {
		return nil, nil
	}
	rows, err := t.NamedQuery(ctx, d, query, args...)
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

func (t *Table[T]) GetSelectableColumns(groupBy bool, names ...db.Column) []string {
	var selectValues []string
	selectableColumns := t.GetColumns()
	if len(names) > 0 {
		for _, name := range names {
			if name.Name == "" {
				continue
			}
			e, found := selectableColumns[name.Name]
			if !found {
				continue
			}

			if e.Select {
				selectValues = append(selectValues, name.FullName(groupBy, true))
			}
		}
		return selectValues
	}

	for _, e := range selectableColumns {
		if e.Select {
			selectValues = append(selectValues, e.FullName(groupBy, true))
		}
	}
	return selectValues
}

func (t *Table[T]) WhereStatement(conditional string, whereElementsStr ...string) string {
	whereValues := t.WhereValues(whereElementsStr...)
	if len(whereValues) == 0 {
		return ""
	}
	if conditional == "" {
		conditional = "AND"
	}
	return fmt.Sprintf("WHERE %s", strings.Join(whereValues, fmt.Sprintf(" %s ", conditional)))
}

func (t *Table[T]) OrderByStatement(groupBy bool, orderBy ...string) string {
	var orderByValues []string

	var columns []db.Column
	c := t.GetColumns()
	for _, o := range orderBy {
		if v, found := c[o]; found {
			columns = append(columns, v)
			//orderByValues = append(orderByValues, v.GetOrderStmt())
		}
	}
	if len(orderBy) == 0 {
		for _, column := range c {
			if column.Order {
				columns = append(columns, column)
			}
		}
	}
	if len(columns) == 0 {
		return ""
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].OrderPriority < columns[j].OrderPriority
	})
	for _, column := range columns {
		orderByValues = append(orderByValues, column.GetOrderStmt(groupBy))
	}

	return fmt.Sprintf("ORDER BY %s", strings.Join(orderByValues, ","))
}

func (t *Table[T]) OrderByColumns(groupBy bool, columns ...db.Column) string {
	var orderByValues []string

	for _, column := range t.Columns {
		if column.Order {
			columns = append(columns, column)
		}
	}

	if len(columns) == 0 {
		return ""
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].OrderPriority < columns[j].OrderPriority
	})
	for _, column := range columns {
		orderByValues = append(orderByValues, column.GetOrderStmt(groupBy))
	}

	return fmt.Sprintf("ORDER BY %s", strings.Join(orderByValues, ","))
}

func (t *Table[T]) IsAutoGenerateID() bool {
	for _, e := range t.Columns {
		if e.AutoGenerateID {
			return true
		}
	}
	return false
}

func (t *Table[T]) GetGenerateID() []db.Column {
	var output []db.Column
	for _, e := range t.Columns {
		if e.AutoGenerateID {
			output = append(output, e)
		}
	}
	return output
}

func (t *Table[T]) GenerateID() map[string]string {
	m := map[string]string{}
	for _, e := range t.GetGenerateID() {
		uid := uuid.New().String()
		switch e.AutoGenerateIDType {
		case "hex":
			hasher := sha1.New()
			hasher.Write([]byte(uid))
			m[e.Name] = hex.EncodeToString(hasher.Sum(nil))
		case "base64":
			hasher := sha1.New()
			hasher.Write([]byte(uid))
			m[e.Name] = base64.URLEncoding.EncodeToString(hasher.Sum(nil))
		case "uuid":
			fallthrough
		default:
			m[e.Name] = uid
		}
	}
	return m
}

func (t *Table[T]) InsertStatement(amount int) string {
	var columnNames []string
	var values []string
	for _, e := range t.GetColumns() {
		if e.Skip {
			continue
		}
		columnNames = append(columnNames, e.Name)
		values = append(values, e.Name)
	}
	if len(columnNames) == 0 {
		return ""
	}
	var rows []string
	for i := 0; i < amount; i++ {
		rows = append(rows, fmt.Sprintf("(%s)", strings.Join(ArrayWithPrefix(fmt.Sprintf(":%d_", i), values), ",")))
	}

	insert := fmt.Sprintf("INSERT INTO %s(%s) VALUES \n%s;",
		t.FullTableName(),
		strings.Join(columnNames, ","), strings.Join(rows, ",\n"))
	return insert
}

func ArrayWithPrefix(prefix string, list []string) []string {
	var rows []string
	for _, i := range list {
		rows = append(rows, fmt.Sprintf("%s%s", prefix, i))
	}
	return rows
}

func (t *Table[T]) UpsertStatement(amount int) string {
	insert := strings.TrimSuffix(t.InsertStatement(amount), ";")
	onDuplicate := `ON DUPLICATE KEY UPDATE`
	var setValues []string

	for _, e := range t.Columns {
		if !e.Update {
			continue
		}
		setValues = append(setValues, fmt.Sprintf("%s = VALUES(%s)", e.Name, e.Name))
	}
	return fmt.Sprintf("%s\n%s\n%s", insert, onDuplicate, strings.Join(setValues, ",\n"))
}

func (t *Table[T]) UpdateStatement() string {
	var setValues []string
	var whereValues []string
	for _, e := range t.Columns {
		if e.Primary && !e.Update {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
		} else if e.AutoGenerateID {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
		}
		if !e.Update {
			continue
		}
		setValues = append(setValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
	}
	if len(setValues) == 0 {
		return ""
	}

	if len(whereValues) == 0 {
		return ""
	}
	update := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		t.FullTableName(),
		strings.Join(setValues, " ,"), strings.Join(whereValues, " AND "))
	return update
}

func DeleteStatement(fullTableName string, columns map[string]db.Column) string {
	var whereValues []string
	for _, e := range columns {
		if e.Primary {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
			continue
		}
		if e.Delete {
			return fmt.Sprintf("DELETE FROM %s WHERE %s = :%s", fullTableName, e.Name, e.Name)
		}
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", fullTableName, strings.Join(whereValues, " AND "))
}

func (t *Table[T]) DeleteWithColumns(ctx context.Context, fullTableName string, columns map[string]db.Column, s T) error {
	if t.db == nil {
		return nil
	}
	if c := t.GetCommonColumns(columns); len(c) == 0 {
		return nil
	}
	tracer := otel.GetTracerProvider()
	ctx, span := tracer.Tracer("delete-w-column").Start(ctx, t.FullTableName())
	defer span.End()
	//tableUpdateSignal <- t.FullTableName()
	return t.db.ExecContext(ctx, DeleteStatement(fullTableName, columns), s)
}

func (t *Table[T]) DeleteStatement() string {
	var whereValues []string
	for _, e := range t.GetColumns() {
		if e.Primary {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
			continue
		}
		if e.Delete {
			return fmt.Sprintf("DELETE FROM %s WHERE %s = :%s", t.FullTableName(), e.Name, e.Name)
		}
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", t.FullTableName(), strings.Join(whereValues, " AND "))
}

func (t *Table[T]) CountStatement(conditional string, whereElementsStr ...string) string {
	wh := t.WhereStatement(conditional, whereElementsStr...)
	return fmt.Sprintf("SELECT COUNT(*) as count FROM %s %s", t.FullTableName(), wh)
}

func (t *Table[T]) NamedQuery(ctx context.Context, d db.DB, query string, args ...interface{}) (db.DBRow, error) {
	if d == nil {
		d = t.db
	}
	if d == nil {
		return nil, nil
	}
	a, err := combineStructs(args...)
	if err != nil {
		return nil, err
	}
	query = fixArrays(query, a)
	return d.QueryContext(ctx, query, a)
}

func (t *Table[T]) NamedExec(ctx context.Context, d db.DB, query string, args ...interface{}) error {
	if d == nil {
		d = t.db
	}
	if d == nil {
		return nil
	}
	a, err := combineStructs(args...)
	if err != nil {
		return err
	}
	query = fixArrays(query, a)
	return d.ExecContext(ctx, query, a)
}

func (t *Table[T]) HasColumn(c db.Column) (string, bool) {
	if !c.Join && !c.Select && c.WhereJoin == "" {
		return "", false
	}
	if c.Table == t.Name && c.Dataset == t.Dataset {
		return "", false
	}
	for _, column := range t.Columns {
		if c.JoinName == column.JoinName && (len(c.JoinName) > 0 && len(column.JoinName) > 0) {
			return c.JoinName, true
		}
		if c.JoinName == column.Name && len(c.JoinName) > 0 {
			return column.Name, true
		}
		if c.Name == column.Name {
			return column.Name, true
		}
		if c.Name == column.JoinName && len(column.JoinName) > 0 {
			return column.Name, true
		}
	}
	return "", false
}

func (t *Table[T]) GetCommonColumns(columns map[string]db.Column) map[string]db.Column {
	overlappingColumns := map[string]db.Column{}
	for k, column := range columns {
		if _, found := t.HasColumn(column); found {
			overlappingColumns[k] = column
		}
	}
	return overlappingColumns
}

func (t *Table[T]) SelectJoinStmt(JoinType string, orderBy []string, groupBy bool, tableColumns ...map[string]db.Column) (string, error) {
	overlappingColumns := map[string]db.Column{}
	allColumns := map[string]db.Column{}
	for _, columns := range tableColumns {
		overlappingColumns = JoinMaps[db.Column](overlappingColumns, t.GetCommonColumns(columns))
		allColumns = JoinMaps[db.Column](allColumns, columns)
	}
	if len(overlappingColumns) == 0 {
		return "", NoOverlappingColumnsErr
	}
	joinStmt := t.generateJoinStmt(overlappingColumns, JoinType)
	whereStmt := t.generateWhereStmt(allColumns)
	columns := t.GetSelectableColumns(groupBy)
	selectStmt := fmt.Sprintf("SELECT %s FROM %s %s %s %s", strings.Join(columns, ","), t.FullTableName(), joinStmt, whereStmt, t.OrderByStatement(false, orderBy...))
	t.OrderByStatement(false)
	return selectStmt, nil
}

func (t *Table[T]) generateJoinStmt(columns map[string]db.Column, JoinType string) string {
	if len(columns) == 0 {
		return ""
	}
	joinExp := "JOIN"
	switch strings.ToLower(JoinType) {
	case "left":
		joinExp = "LEFT JOIN"
	case "right":
		joinExp = "RIGHT JOIN"
	}
	tableJoins := map[string][]string{}
	for _, column := range columns {
		name, found := t.HasColumn(column)
		if !column.Join || !found {
			continue
		}
		if _, found := tableJoins[column.FullTableName()]; !found {
			tableJoins[column.FullTableName()] = []string{}
			joinStmt := fmt.Sprintf("%s %s ON %s.%s = %s.%s", joinExp, column.FullTableName(), column.Table, column.Name, t.Name, name)
			tableJoins[column.FullTableName()] = append(tableJoins[column.FullTableName()], joinStmt)
		} else {
			joinStmt := fmt.Sprintf("%s.%s = %s.%s", column.Table, column.Name, t.Name, name)
			tableJoins[column.FullTableName()] = append(tableJoins[column.FullTableName()], joinStmt)
		}
	}

	output := ""
	for _, v := range tableJoins {
		output += strings.Join(v, " AND ") + "\n"
	}
	return output
}

func (t *Table[T]) generateWhereStmt(columns map[string]db.Column) string {
	if len(columns) == 0 {
		return ""
	}
	stmts := WhereValues(columns, true)
	if len(stmts) == 0 {
		return ""
	}
	return fmt.Sprintf(" WHERE %s", strings.Join(stmts, " AND "))
}

func (t *Table[T]) Insert(ctx context.Context, d db.DB, s ...T) (string, error) {
	if d == nil {
		d = t.db
	}
	if d == nil {
		return "", nil
	}
	tracer := otel.GetTracerProvider()
	ctx, span := tracer.Tracer("insert").Start(ctx, t.FullTableName())
	defer span.End()
	if t.IsAutoGenerateID() {
		generateIds := t.GenerateID()
		args := map[string]interface{}{}
		for rowIndex, i := range s {
			tmpArgs, err := combineStructs(generateIds, i)
			if err != nil {
				return "", err
			}
			tmpArgs = AddPrefix(fmt.Sprintf("%d_", rowIndex), tmpArgs)
			args, err = combineStructs(args, tmpArgs)
			if err != nil {
				return "", err
			}
		}
		err := d.ExecContext(ctx, t.InsertStatement(len(s)), args)
		if err == nil {
			span.RecordError(err)
			_ = cachec.GlobalCacheMonitor.UpdateCache(ctx, t.FullTableName(), "insert")
		}
		return generateIds[t.GetGenerateID()[0].Name], err
	}

	args, err := combineStructsWithPrefix[T](s...)
	if err != nil {
		span.RecordError(err)
		return "", err
	}
	err = d.ExecContext(ctx, t.InsertStatement(len(s)), args)
	if err == nil {
		span.RecordError(err)
		_ = cachec.GlobalCacheMonitor.UpdateCache(ctx, t.FullTableName(), "insert")
	}
	return "", err
}

func (t *Table[T]) Upsert(ctx context.Context, d db.DB, s ...T) (string, error) {
	if d == nil {
		d = t.db
	}
	if d == nil {
		return "", nil
	}
	tracer := otel.GetTracerProvider()
	ctx, span := tracer.Tracer("upsert").Start(ctx, t.FullTableName())
	defer span.End()
	if t.IsAutoGenerateID() {
		generateIds := t.GenerateID()
		args := map[string]interface{}{}
		for rowIndex, i := range s {
			tmpArgs, err := combineStructs(generateIds, i)
			if err != nil {
				return "", err
			}
			tmpArgs = AddPrefix(fmt.Sprintf("%d_", rowIndex), tmpArgs)
			args, err = combineStructs(args, tmpArgs)
			if err != nil {
				return "", err
			}
		}
		err := d.ExecContext(ctx, t.UpsertStatement(len(s)), args)
		if err == nil {
			span.RecordError(err)
			_ = cachec.GlobalCacheMonitor.UpdateCache(ctx, t.FullTableName(), "insert")
		}
		return generateIds[t.GetGenerateID()[0].Name], err
	}
	args, err := combineStructsWithPrefix[T](s...)
	if err != nil {
		return "", err
	}
	err = d.ExecContext(ctx, t.UpsertStatement(len(s)), args)
	if err == nil {
		span.RecordError(err)
		_ = cachec.GlobalCacheMonitor.UpdateCache(ctx, t.FullTableName(), "insert")
	}
	return "", err
}

func (t *Table[T]) InsertTx(ctx context.Context, db *sqlx.Tx, s ...T) (sql.Result, string, error) {
	if db == nil {
		return nil, "", nil
	}
	if t.IsAutoGenerateID() {
		generateIds := t.GenerateID()
		args, err := combineStructs(generateIds, s)
		if err != nil {
			return nil, "", err
		}
		results, err := db.NamedExecContext(ctx, t.InsertStatement(len(s)), args)
		return results, generateIds[t.GetGenerateID()[0].Name], err
	}
	results, err := db.NamedExecContext(ctx, t.InsertStatement(len(s)), s)
	return results, "", err
}

func (t *Table[T]) Delete(ctx context.Context, d db.DB, s T) error {
	if d == nil {
		d = t.db
	}
	if d == nil {
		return nil
	}
	tracer := otel.GetTracerProvider()
	ctx, span := tracer.Tracer("delete").Start(ctx, t.FullTableName())
	defer span.End()
	err := d.ExecContext(ctx, t.DeleteStatement(), s)
	if err != nil {
		span.RecordError(err)
		return err
	}
	_ = cachec.GlobalCacheMonitor.UpdateCache(ctx, t.FullTableName(), "insert")
	return nil
}

func (t *Table[T]) DeleteTx(ctx context.Context, db *sqlx.Tx, s T) (sql.Result, error) {
	if db == nil {
		return nil, nil
	}
	r, err := db.NamedExecContext(ctx, t.DeleteStatement(), s)
	if err != nil {
		return r, err
	}
	_ = cachec.GlobalCacheMonitor.UpdateCache(ctx, t.FullTableName(), "insert")
	return r, nil
}

func (t *Table[T]) Update(ctx context.Context, d db.DB, s T) error {
	if d == nil {
		d = t.db
	}
	if d == nil {
		return nil
	}
	tracer := otel.GetTracerProvider()
	ctx, span := tracer.Tracer("update").Start(ctx, t.FullTableName())
	defer span.End()
	err := d.ExecContext(ctx, t.UpdateStatement(), s)
	if err != nil {
		span.RecordError(err)
		return err
	}
	_ = cachec.GlobalCacheMonitor.UpdateCache(ctx, t.FullTableName(), "insert")
	return nil
}

func (t *Table[T]) UpdateTx(ctx context.Context, db *sqlx.Tx, s T) (sql.Result, error) {
	if db == nil {
		return nil, nil
	}
	//tableUpdateSignal <- t.FullTableName()
	r, err := db.ExecContext(ctx, t.UpdateStatement(), s)
	if err != nil {
		return nil, err
	}
	_ = cachec.GlobalCacheMonitor.UpdateCache(ctx, t.FullTableName(), "insert")
	return r, nil
}

func NamedQuery(ctx context.Context, d db.DB, query string, args ...interface{}) (db.DBRow, error) {
	if d == nil {
		return nil, nil
	}
	a, err := combineStructs(args...)
	if err != nil {
		return nil, err
	}
	query = fixArrays(query, a)
	return d.QueryContext(ctx, query, a)
}
