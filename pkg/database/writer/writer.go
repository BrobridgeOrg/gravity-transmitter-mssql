package writer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-transmitter-mssql/pkg/database"
	buffered_input "github.com/cfsghost/buffered-input"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var seq uint64

var (
	UpdateTemplate = `UPDATE %s SET %s WHERE "%s" = :primary_val`
	//UpdateTemplate = "UPDATE `%s` SET %s WHERE `%s` = :primary_val"
	InsertTemplate = `INSERT INTO %s (%s) VALUES (%s)`
	//InsertTemplate = "INSERT INTO `%s` (%s) VALUES (%s)"
	DeleteTemplate = `DELETE FROM %s WHERE "%s" = :primary_val`
	//DeleteTemplate = "DELETE FROM `%s` WHERE `%s` = :primary_val"
)

var recordDefPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_record.RecordDef{}
	},
}

type DatabaseInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Secure   bool   `json:"secure"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"dbname"`
}

type RecordDef struct {
	HasPrimary    bool
	PrimaryColumn string
	Values        map[string]interface{}
	ColumnDefs    []*ColumnDef
}

type ColumnDef struct {
	ColumnName  string
	BindingName string
	Value       interface{}
}

type Writer struct {
	dbInfo            *DatabaseInfo
	db                *sqlx.DB
	commands          chan *DBCommand
	completionHandler database.CompletionHandler
	buffer            *buffered_input.BufferedInput
	tmpQueryStr       string
}

func NewWriter() *Writer {
	writer := &Writer{
		dbInfo:            &DatabaseInfo{},
		commands:          make(chan *DBCommand, 2048),
		completionHandler: func(database.DBCommand) {},
		tmpQueryStr:       "",
	}
	// Initializing buffered input
	opts := buffered_input.NewOptions()
	opts.ChunkSize = viper.GetInt("bufferInput.chunkSize")
	opts.ChunkCount = 10000
	opts.Timeout = viper.GetDuration("bufferInput.timeout") * time.Millisecond
	opts.Handler = writer.chunkHandler
	writer.buffer = buffered_input.NewBufferedInput(opts)

	return writer
}

func (writer *Writer) Init() error {

	// Read configuration file
	writer.dbInfo.Host = viper.GetString("database.host")
	writer.dbInfo.Port = viper.GetInt("database.port")
	writer.dbInfo.Secure = viper.GetBool("database.secure")
	writer.dbInfo.Username = viper.GetString("database.username")
	writer.dbInfo.Password = viper.GetString("database.password")
	writer.dbInfo.DbName = viper.GetString("database.dbname")

	log.WithFields(log.Fields{
		"host":     writer.dbInfo.Host,
		"port":     writer.dbInfo.Port,
		"secure":   writer.dbInfo.Secure,
		"username": writer.dbInfo.Username,
		"dbname":   writer.dbInfo.DbName,
	}).Info("Connecting to database")

	tlsmode := "false"
	if writer.dbInfo.Secure {
		tlsmode = "true"
	}

	connStr := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=%s&tls=%s",
		writer.dbInfo.Username,
		writer.dbInfo.Password,
		writer.dbInfo.Host,
		writer.dbInfo.Port,
		writer.dbInfo.DbName,
		tlsmode,
	)

	// Open database
	db, err := sqlx.Open("sqlserver", connStr)
	if err != nil {
		log.Error(err)
		return err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	writer.db = db

	go writer.run()

	return nil
}

func (writer *Writer) chunkHandler(chunk []interface{}) {

	dbCommands := make([]*DBCommand, 0, len(chunk))
	for _, request := range chunk {

		dbCommands = append(dbCommands, request.(*DBCommand))
	}

	writer.processData(dbCommands)
}

func (writer *Writer) processInsertData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	if writer.tmpQueryStr != cmd.QueryStr {
		writer.tmpQueryStr = cmd.QueryStr
		qStr, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
		newQueryStr := ""
		lastIndex := 0
		for i := 1; i <= len(arg); i++ {
			newSeq := atomic.AddUint64((*uint64)(&seq), 1)
			key := fmt.Sprintf("%v%d", "@p", i)
			newKey := fmt.Sprintf("%v%d", "@p", newSeq)

			if i == 1 {
				index := strings.Index(qStr, key)
				newQueryStr = fmt.Sprintf("%v%v", qStr[:index], newKey)
				lastIndex = index + len(key)
			} else {
				qStr = qStr[lastIndex:]
				index := strings.Index(qStr, key)
				if index == -1 {
					continue
				}
				newQueryStr = fmt.Sprintf("%v%v%v", newQueryStr, qStr[:index], newKey)
				lastIndex = index + len(key)
				if i == len(arg) {
					newQueryStr = fmt.Sprintf("%v%v", newQueryStr, qStr[lastIndex:])
				}
			}
		}
		querys = append(querys, newQueryStr)
		args = append(args, arg...)
	} else {

		querys, args = writer.appendInsertData(cmd, querys, args)
	}

	return querys, args

}

func (writer *Writer) appendInsertData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	_, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
	var addVal []string
	for i := 1; i <= len(arg); i++ {
		newSeq := atomic.AddUint64((*uint64)(&seq), 1)
		newKey := fmt.Sprintf("%v%d", "@p", newSeq)
		addVal = append(addVal, newKey)
	}
	addVals := strings.Join(addVal, ",")
	newQuery := fmt.Sprintf("%s,(%s)", querys[len(querys)-1], addVals)
	querys[len(querys)-1] = newQuery
	args = append(args, arg...)

	return querys, args
}

func (writer *Writer) processUpdateData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	writer.tmpQueryStr = cmd.QueryStr
	qStr, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
	newQueryStr := ""
	lastIndex := 0

	for i := 1; i <= len(arg); i++ {
		newSeq := atomic.AddUint64((*uint64)(&seq), 1)
		key := fmt.Sprintf("%v%d", "@p", i)
		newKey := fmt.Sprintf("%v%d", "@p", newSeq)

		if i == 1 {
			index := strings.Index(qStr, key)
			newQueryStr = fmt.Sprintf("%v%v", qStr[:index], newKey)
			lastIndex = index + len(key)
		} else {
			qStr = qStr[lastIndex:]
			index := strings.Index(qStr, key)
			if index == -1 {
				continue
			}

			newQueryStr = fmt.Sprintf("%v%v%v", newQueryStr, qStr[:index], newKey)
			lastIndex = index + len(key)
		}
	}

	querys = append(querys, newQueryStr)
	args = append(args, arg...)

	return querys, args
}

func (writer *Writer) processDeleteData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	if writer.tmpQueryStr != cmd.QueryStr {
		writer.tmpQueryStr = cmd.QueryStr
		qStr, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
		qStr = fmt.Sprintf("%v;", qStr)
		for i := 1; i <= len(arg); i++ {
			newSeq := atomic.AddUint64((*uint64)(&seq), 1)
			if i == len(arg) {
				key := fmt.Sprintf(" %v%d;", "@p", i)
				newKey := fmt.Sprintf(" %v%d;", "@p", newSeq)
				if key != newKey {
					qStr = strings.Replace(qStr, key, newKey, 1)
				}
			}
		}

		qStr = strings.TrimRight(qStr, ";")
		querys = append(querys, qStr)
		args = append(args, arg...)

	} else {

		querys, args = writer.appendDeleteData(cmd, querys, args)
	}

	return querys, args

}

func (writer *Writer) appendDeleteData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	_, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
	qStr := querys[len(querys)-1]
	qStr = fmt.Sprintf("%v;", qStr)
	if strings.Index(qStr, ");") == -1 {
		lastKey := fmt.Sprintf("%v%d", "@p", seq)
		lastStr := fmt.Sprintf(" = %v;", lastKey)

		newSeq := atomic.AddUint64((*uint64)(&seq), 1)
		newKey := fmt.Sprintf("%v%d", "@p", newSeq)
		inStr := fmt.Sprintf(" IN (%v,%v)", lastKey, newKey)

		qStr = strings.Replace(qStr, lastStr, inStr, 1)

	} else {
		newSeq := atomic.AddUint64((*uint64)(&seq), 1)
		newKey := fmt.Sprintf(",%v%d)", "@p", newSeq)
		qStr = strings.Replace(qStr, ");", newKey, 1)
	}

	querys[len(querys)-1] = qStr
	args = append(args, arg[len(arg)-1])

	return querys, args
}

func (writer *Writer) processData(dbCommands []*DBCommand) {
	// Write to Database
	for {
		var args []interface{}
		var querys []string
		writer.tmpQueryStr = ""
		seq = 0
		for _, cmd := range dbCommands {

			switch cmd.Record.Method {
			case gravity_sdk_types_record.Method_INSERT:
				querys, args = writer.processInsertData(cmd, querys, args)

			case gravity_sdk_types_record.Method_UPDATE:
				querys, args = writer.processUpdateData(cmd, querys, args)

			case gravity_sdk_types_record.Method_DELETE:
				querys, args = writer.processDeleteData(cmd, querys, args)

			}

		}

		// Write to batch
		queryStr := strings.Join(querys, ";")

		_, err := writer.db.Exec(queryStr, args...)
		if err != nil {
			log.Error(err)
			log.Error(queryStr)
			log.Error(args...)

			<-time.After(time.Second * 5)

			log.WithFields(log.Fields{}).Warn("Retry to write record to database by batch ...")
			continue
		}

		break
	}

	for _, cmd := range dbCommands {
		writer.completionHandler(database.DBCommand(cmd))
		recordDefPool.Put(cmd.RecordDef)
		dbCommandPool.Put(cmd)
	}
}

func (writer *Writer) run() {
	for {
		select {
		case cmd := <-writer.commands:
			// publish to buffered-input
			writer.buffer.Push(cmd)
		}
	}
}

func (writer *Writer) SetCompletionHandler(fn database.CompletionHandler) {
	writer.completionHandler = fn
}

func (writer *Writer) ProcessData(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	/*
		log.WithFields(log.Fields{
			"method": record.Method,
			"event":  record.EventName,
			"table":  record.Table,
		}).Info("Write record")
	*/

	switch record.Method {
	case gravity_sdk_types_record.Method_DELETE:
		return writer.DeleteRecord(reference, record, tables)
	case gravity_sdk_types_record.Method_UPDATE:
		return writer.UpdateRecord(reference, record, tables)
	case gravity_sdk_types_record.Method_INSERT:
		return writer.InsertRecord(reference, record, tables)
	}

	return nil
}

func (writer *Writer) GetDefinition(record *gravity_sdk_types_record.Record) (*gravity_sdk_types_record.RecordDef, error) {

	recordDef := recordDefPool.Get().(*gravity_sdk_types_record.RecordDef)
	recordDef.HasPrimary = false
	recordDef.Values = make(map[string]interface{})
	recordDef.ColumnDefs = make([]*gravity_sdk_types_record.ColumnDef, 0, len(record.Fields))

	// Scanning fields
	for n, field := range record.Fields {

		value := gravity_sdk_types_record.GetValue(field.Value)

		// Primary key
		//if field.IsPrimary == true {
		if record.PrimaryKey == field.Name {
			recordDef.Values["primary_val"] = value
			recordDef.HasPrimary = true
			recordDef.PrimaryColumn = field.Name
			continue
		}

		// Generate binding name
		bindingName := fmt.Sprintf("val_%s", strconv.Itoa(n))
		recordDef.Values[bindingName] = value

		// Store definition
		recordDef.ColumnDefs = append(recordDef.ColumnDefs, &gravity_sdk_types_record.ColumnDef{
			ColumnName:  field.Name,
			Value:       field.Name,
			BindingName: bindingName,
		})
	}

	if len(record.PrimaryKey) > 0 && !recordDef.HasPrimary {
		log.WithFields(log.Fields{
			"column": record.PrimaryKey,
		}).Error("Not found primary key")

		return nil, errors.New("Not found primary key")
	}

	return recordDef, nil
}

func (writer *Writer) InsertRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	return writer.insert(reference, record, record.Table, recordDef, tables)
}

func (writer *Writer) UpdateRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	// Ignore if no primary key
	if recordDef.HasPrimary == false {
		return nil
	}

	_, err = writer.update(reference, record, record.Table, recordDef, tables)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) DeleteRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	if record.PrimaryKey == "" {
		// Do nothing
		return nil
	}

	for _, field := range record.Fields {

		// Primary key
		//if field.IsPrimary == true {
		if record.PrimaryKey == field.Name {

			value := gravity_sdk_types_record.GetValue(field.Value)

			sqlStr := fmt.Sprintf(DeleteTemplate, record.Table, field.Name)

			dbCommand := dbCommandPool.Get().(*DBCommand)
			dbCommand.Reference = reference
			dbCommand.Record = record
			dbCommand.QueryStr = sqlStr
			dbCommand.Args = map[string]interface{}{
				"primary_val": value,
			}
			dbCommand.Tables = tables

			writer.commands <- dbCommand

			break
		}
	}

	return nil
}

func (writer *Writer) update(reference interface{}, record *gravity_sdk_types_record.Record, table string, recordDef *gravity_sdk_types_record.RecordDef, tables []string) (bool, error) {

	// Preparing SQL string
	updates := make([]string, 0, len(recordDef.ColumnDefs))
	for _, def := range recordDef.ColumnDefs {
		//updates = append(updates, "`"+def.ColumnName+"` = :"+def.BindingName)
		updates = append(updates, def.ColumnName+" = :"+def.BindingName)
	}

	updateStr := strings.Join(updates, ",")
	sqlStr := fmt.Sprintf(UpdateTemplate, table, updateStr, recordDef.PrimaryColumn)

	dbCommand := dbCommandPool.Get().(*DBCommand)
	dbCommand.Reference = reference
	dbCommand.Record = record
	dbCommand.QueryStr = sqlStr
	dbCommand.Args = recordDef.Values
	dbCommand.RecordDef = recordDef
	dbCommand.Tables = tables

	writer.commands <- dbCommand

	return false, nil
}

func (writer *Writer) insert(reference interface{}, record *gravity_sdk_types_record.Record, table string, recordDef *gravity_sdk_types_record.RecordDef, tables []string) error {

	paramLength := len(recordDef.ColumnDefs)
	if recordDef.HasPrimary {
		paramLength++
	}

	// Allocation
	colNames := make([]string, 0, paramLength)
	valNames := make([]string, 0, paramLength)

	if recordDef.HasPrimary {
		//colNames = append(colNames, "`"+recordDef.PrimaryColumn+"`")
		colNames = append(colNames, recordDef.PrimaryColumn)
		valNames = append(valNames, ":primary_val")
	}

	// Preparing columns and bindings
	for _, def := range recordDef.ColumnDefs {
		//colNames = append(colNames, "`"+def.ColumnName+"`")
		colNames = append(colNames, def.ColumnName)
		valNames = append(valNames, `:`+def.BindingName)
	}

	// Preparing SQL string to insert
	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")
	insertStr := fmt.Sprintf(InsertTemplate, table, colsStr, valsStr)

	//	database.db.NamedExec(insertStr, recordDef.Values)
	dbCommand := dbCommandPool.Get().(*DBCommand)
	dbCommand.Reference = reference
	dbCommand.Record = record
	dbCommand.QueryStr = insertStr
	dbCommand.Args = recordDef.Values
	dbCommand.RecordDef = recordDef
	dbCommand.Tables = tables

	writer.commands <- dbCommand

	return nil
}
