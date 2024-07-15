package sqlc

import (
	"context"
	"fmt"
	"github.com/Seann-Moser/cutil/logc"
	"github.com/Seann-Moser/cutil/sqlc/orm"
	"github.com/Seann-Moser/cutil/sqlc/orm/db"
	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"
	"net/http"
	"reflect"
	"time"
)

type DAO struct {
	db            db.DB
	updateColumns bool
	ctx           context.Context
	tablesNames   []string
	tableColumns  map[string]map[string]db.Column
}

func NewSQLDao(ctx context.Context) (*DAO, error) {
	dao, err := connectToDB(
		ctx,
		viper.GetString(DBUserNameFlag),
		viper.GetString(DBPasswordFlag),
		viper.GetString(DBHostFlag),
		viper.GetInt(DBPortFlag),
		viper.GetInt(DBMaxConnectionsFlag),
		viper.GetInt(DBMaxIdleConnectionsFlag),
		viper.GetDuration(DBMaxConnectionLifetime),
	)
	if err != nil {
		return nil, err
	}
	d := db.NewSql(dao)
	return &DAO{
		ctx:           orm.AddDBContext(ctx, "", d),
		db:            d,
		updateColumns: viper.GetBool(DBUpdateTablesFlag),
		tablesNames:   make([]string, 0),
		tableColumns:  map[string]map[string]db.Column{},
	}, nil
}

func (d *DAO) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d.ctx != nil {
			daoCtx := context.WithValue(r.Context(), "go-serve-dao", d) //nolint staticcheck
			ctx, err := orm.WithTableContext(daoCtx, d.ctx, d.tablesNames...)
			if err == nil {
				r = r.WithContext(ctx)
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (d *DAO) Close() {
	d.db.Close()

}

func (d *DAO) GetContext() context.Context {
	return d.ctx
}

func (d *DAO) SetContext(ctx context.Context) context.Context {
	d.ctx = ctx
	return d.ctx
}

func (d *DAO) AddTablesToCtx(ctx context.Context) context.Context {
	if d.ctx != nil {
		daoCtx := context.WithValue(ctx, "go-serve-dao", d) //nolint staticcheck
		ctx, err := orm.WithTableContext(daoCtx, d.ctx, d.tablesNames...)
		if err == nil {
			return ctx
		}
	}
	return d.ctx
}

func AddTable[T any](dao *DAO, datasetName string, queryType orm.QueryType) error {
	tmpCtx, err := orm.AddTableCtx[T](dao.GetContext(), dao.db, datasetName, queryType)
	if err != nil {
		var t T
		return fmt.Errorf("failed creating table(%s): %w", getType(t), err)
	}
	table, err := GetTableCtx[T](tmpCtx)
	if err != nil {
		return fmt.Errorf("failed getting table from context")
	}
	dao.tablesNames = append(dao.tablesNames, table.Name)
	if _, found := dao.tableColumns[table.FullTableName()]; !found {
		dao.tableColumns[table.FullTableName()] = table.Columns
	}

	dao.SetContext(tmpCtx)
	return nil
}

func getType(myVar interface{}) string {
	if t := reflect.TypeOf(myVar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

func connectToDB(ctx context.Context, user, password, host string, port, maxConnections, idleConn int, lifeTime time.Duration) (*sqlx.DB, error) {
	dbConf := mysql.Config{
		AllowNativePasswords:    true,
		User:                    user,
		Passwd:                  password,
		Net:                     "tcp",
		Addr:                    fmt.Sprintf("%s:%d", host, port),
		CheckConnLiveness:       true,
		AllowCleartextPasswords: true,
		MaxAllowedPacket:        4 << 20,
	}
	logc.Info(ctx, "connecting to db", zap.String("dsn", dbConf.FormatDSN()))

	otelSql, err := otelsql.Open("mysql", dbConf.FormatDSN(), otelsql.WithAttributes(
		semconv.DBSystemMySQL))
	if err != nil {
		return nil, err
	}
	db := sqlx.NewDb(otelSql, "mysql")
	db.SetMaxOpenConns(maxConnections)
	db.SetConnMaxLifetime(lifeTime)
	db.SetMaxIdleConns(idleConn)
	if err = db.Ping(); err == nil {
		return db, nil
	}
	var retries int
	ticker := time.NewTicker(5 * time.Second)
	err = otelsql.RegisterDBStatsMetrics(otelSql, otelsql.WithAttributes(
		semconv.DBSystemMySQL,
	))
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled")
		case <-ticker.C:
			if retries >= viper.GetInt(DBMaxConnectionRetryFlag) {
				return nil, err
			}
			if err = db.Ping(); err == nil {
				return db, nil
			}
			logc.Info(ctx, "attempting to connect to db", zap.Int("attempt", retries), zap.String("dsn", dbConf.FormatDSN()))
			retries++
		}
	}

}
