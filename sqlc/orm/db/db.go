package db

import (
	"context"
	"github.com/xwb1989/sqlparser"
	"regexp"
	"strings"
)

var (
	// List of reserved keywords
	reservedKeywords = []string{
		"ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "BACKUP", "BETWEEN",
		"CASE", "CHECK", "COLUMN", "CONSTRAINT", "CREATE", "DATABASE", "DEFAULT",
		"DELETE", "DESC", "DISTINCT", "DROP", "EXEC", "EXISTS", "FOREIGN", "FROM",
		"FULL", "GROUP", "HAVING", "IN", "INDEX", "INNER", "INSERT", "IS", "JOIN",
		"KEY", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "OR", "ORDER", "OUTER",
		"PRIMARY", "PROCEDURE", "RIGHT", "ROWNUM", "SELECT", "SET", "TABLE", "TOP",
		"TRUNCATE", "UNION", "UNIQUE", "UPDATE", "VALUES", "VIEW", "WHERE",
	}

	// Regex patterns for invalid column names
	patterns = []string{
		`^\d+.*`,     // Starts with a number
		`.*\s+.*`,    // Contains space
		`.*[-\.@].*`, // Contains dash, dot, or at-sign
	}
)

type DB interface {
	Ping(ctx context.Context) error
	CreateTable(ctx context.Context, dataset, table string, columns map[string]Column) error
	QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error)
	ExecContext(ctx context.Context, query string, args interface{}) error
	Close()
	GetDataset(ds string) string
}

type DBRow interface {
	Next() bool
	StructScan(i interface{}) error
}

func isSQLValid(sql string) (bool, error) {
	_, err := sqlparser.Parse(sql)
	if err != nil {
		return false, err
	}
	return true, nil
}

func isValidColumnName(columnName string) bool {
	for _, keyword := range reservedKeywords {
		if strings.ToUpper(columnName) == keyword {
			return false
		}
	}

	for _, pattern := range patterns {
		matched, _ := regexp.MatchString(pattern, columnName)
		if matched {
			return false
		}
	}
	return true
}
