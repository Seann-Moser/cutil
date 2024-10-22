package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/Seann-Moser/cutil/logc"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"os"
	"sort"
	"strings"
)

var _ DB = &SqlDB{}

type SqlDB struct {
	sql           *sqlx.DB
	updateColumns bool
	tablePrefix   string
}

var (
	MissingPrimaryKeyErr = errors.New("no field was set as the primary key")
)

func Flags() *pflag.FlagSet {
	fs := pflag.NewFlagSet("sql-db", pflag.ExitOnError)
	fs.Bool("sql-db-update-Columns", false, "")
	fs.String("sql-db-Prefix", "", "")
	return fs
}
func NewSql(db *sqlx.DB) *SqlDB {
	return &SqlDB{
		sql:           db,
		updateColumns: viper.GetBool("sql-db-update-Columns"),
		tablePrefix:   viper.GetString("sql-db-Prefix"),
	}
}

func (s *SqlDB) Ping(ctx context.Context) error {
	return s.sql.PingContext(ctx)
}

func (s *SqlDB) Close() {
	_ = s.sql.Close()
}

func (s *SqlDB) GetDataset(ds string) string {
	return fmt.Sprintf("%s%s", s.tablePrefix, ds)
}

func (s *SqlDB) CreateTable(ctx context.Context, dataset, table string, columns map[string]Column) error {
	createSchemaStatement := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dataset)
	var PrimaryKeys []string
	var FK []string
	createStatement := ""
	createStatement += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s(", dataset, table)

	var c []Column

	for _, column := range columns {
		c = append(c, column)
	}

	sort.Slice(c, func(i, j int) bool {
		return c[i].ColumnOrder < c[j].ColumnOrder
	})

	for _, column := range c {
		createStatement += column.GetDefinition() + ","
		if column.HasFK() {
			FK = append(FK, column.GetFK())
		}
		if column.Primary {
			PrimaryKeys = append(PrimaryKeys, column.Name)
		}
	}
	if len(PrimaryKeys) == 0 {
		return MissingPrimaryKeyErr
	} else if len(PrimaryKeys) == 1 {
		createStatement += fmt.Sprintf("\n\tPRIMARY KEY(%s)", PrimaryKeys[0])
	} else {
		createStatement += fmt.Sprintf("\n\tCONSTRAINT PK_%s_%s PRIMARY KEY (%s)", dataset, table, strings.Join(PrimaryKeys, ","))

	}
	if len(FK) > 0 {
		createStatement += "," + strings.Join(FK, ",")
	}
	createStatement += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"

	for _, stmt := range []string{createSchemaStatement, createStatement} {
		_, err := s.sql.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
	}
	if s.updateColumns {
		return s.ColumnUpdater(ctx, dataset, table, columns)
	}
	return nil
}

func (s *SqlDB) QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error) {
	defer func() { //catch or finally
		if err := recover(); err != nil { //catch
			fmt.Fprintf(os.Stderr, "Exception: %v\n", err)
			os.Exit(1)
		}
	}()
	return s.sql.NamedQueryContext(ctx, query, args)
}

func (s *SqlDB) ExecContext(ctx context.Context, query string, args interface{}) error {
	defer func() { //catch or finally
		if err := recover(); err != nil { //catch
			fmt.Fprintf(os.Stderr, "Exception: %v\n", err)
			os.Exit(1)
		}
	}()
	_, err := s.sql.NamedExecContext(ctx, query, args)
	return err
}

func (s *SqlDB) ColumnUpdater(ctx context.Context, dataset, table string, columns map[string]Column) error {
	cols, err := getColumns(ctx, s.sql, dataset, table)
	if err != nil {
		return err
	}
	var addColumns []*Column
	var removeColumns []*sql.ColumnType
	colMap := map[string]*sql.ColumnType{}
	for _, c := range cols {
		colMap[c.Name()] = c
	}

	for _, e := range columns {
		if _, found := colMap[e.Name]; !found {
			addColumns = append(addColumns, &e)
		}
	}

	for _, c := range cols {
		if _, found := columns[c.Name()]; !found {
			removeColumns = append(removeColumns, c)
		}
	}

	alterTable := fmt.Sprintf("ALTER TABLE %s.%s ", dataset, table)

	if len(addColumns) > 0 {
		addStmt := generateColumnStatements(alterTable, "add", addColumns)
		logc.Debug(ctx, "adding Columns to table", zap.String("query", addStmt))
		_, err := s.sql.ExecContext(ctx, addStmt)
		if err != nil {
			return err
		}
	}
	if len(removeColumns) > 0 {
		removeStmt := generateColumnTypeStatements(alterTable, "remove", removeColumns)
		logc.Debug(ctx, "removing Columns from table", zap.String("table", table), zap.String("query", removeStmt))
		_, err := s.sql.ExecContext(ctx, removeStmt)
		if err != nil {
			return err
		}

	}
	return nil
}

func getColumns(ctx context.Context, db *sqlx.DB, dataset, table string) ([]*sql.ColumnType, error) {
	if db == nil {
		return nil, nil
	}
	rows, err := db.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s limit 1;", dataset, table))
	if err != nil {
		return nil, err
	}
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	return cols, nil
}

func generateColumnTypeStatements(alterTable, columnType string, e []*sql.ColumnType) string {
	output := []string{}
	for _, el := range e {
		output = append(output, generateColumnTypeStmt(columnType, el))
	}
	return fmt.Sprintf("%s %s;", alterTable, strings.Join(output, ","))

}

func generateColumnStatements(alterTable, columnType string, e []*Column) string {
	output := []string{}
	for _, el := range e {
		output = append(output, generateColumnStmt(columnType, el))
	}
	return fmt.Sprintf("%s %s;", alterTable, strings.Join(output, ","))

}
func generateColumnStmt(columnType string, e *Column) string {
	switch strings.ToLower(columnType) {
	case "drop":
		return fmt.Sprintf("DROP COLUMN %s;", e.Name)
	case "add":
		return fmt.Sprintf("ADD %s", e.GetDefinition())
	}
	return ""
}

func generateColumnTypeStmt(columnType string, e *sql.ColumnType) string {
	switch strings.ToLower(columnType) {
	case "drop":
		return fmt.Sprintf("DROP COLUMN %s", e.Name())
	case "add":
		return fmt.Sprintf("ADD %s", e.Name())
	}
	return ""
}
