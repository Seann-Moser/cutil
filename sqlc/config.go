package sqlc

import (
	"github.com/Seann-Moser/cutil/sqlc/orm/db"
	"github.com/spf13/pflag"
	"time"
)

const (
	DBUserNameFlag           = "db-user"
	DBPasswordFlag           = "db-password"
	DBHostFlag               = "db-host"
	DBPortFlag               = "db-port"
	DBMaxConnectionsFlag     = "db-max-connections"
	DBUpdateTablesFlag       = "db-update-table"
	DBMaxConnectionRetryFlag = "db-max-connection-retry"
	DBInstanceName           = "db-instance-name"
	DBWriteStatDuration      = "db-write-stat-interval"
	DBMaxIdleConnectionsFlag = "db-max-idle-connections-flag"
	DBMaxConnectionLifetime  = "db-max-connection-lifetime"
)

func Flags() *pflag.FlagSet {
	fs := pflag.NewFlagSet("db-dao", pflag.ExitOnError)
	fs.AddFlagSet(db.Flags())
	fs.String(DBUserNameFlag, "", "")
	fs.String(DBPasswordFlag, "", "")
	fs.String(DBHostFlag, "mysql", "")

	fs.Int(DBPortFlag, 3306, "")
	fs.Int(DBMaxConnectionsFlag, 10, "")
	fs.Int(DBMaxIdleConnectionsFlag, 10, "")
	fs.Int(DBMaxConnectionRetryFlag, 10, "")
	fs.Bool(DBUpdateTablesFlag, false, "")
	fs.Duration(DBMaxConnectionLifetime, 1*time.Minute, "")

	return fs
}
