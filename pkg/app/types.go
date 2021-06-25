package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-mssql/pkg/database"
)

type App interface {
	GetWriter() database.Writer
}
