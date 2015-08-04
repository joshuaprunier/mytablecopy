package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"time"

	"golang.org/x/crypto/ssh/terminal"

	_ "github.com/go-sql-driver/mysql"
)

const (
	// Insert buffer size
	insertBufferSize = 1048576 // 1MB

	// Timeout length where ctrl+c is ignored.
	signalTimeout = 3 // Seconds
)

// Type definitions
type (
	// dbInfo contains information about a database
	dbInfo struct {
		db      *sql.DB
		user    string
		pass    string
		host    string
		port    string
		sock    string
		schema  string
		table   string
		where   string
		columns []string
	}
)

// Version information supplied by build script
var versionInformation string

// ShowUsage prints a help screen
func showUsage() {
	fmt.Printf("\tmytablecopy version %s\n", versionInformation)
	fmt.Println(`
	USAGE:
	mytablecopy -srcuser=jprunier -srcpass= -srchost=megaman -srctable=srchOpt.company -tgthost=inf-dev-db-0
	mytablecopy -srcuser=jprunier -srcpass=mypass -srchost=megaman -srctable=srchOpt.company -where="1=1 limit 1000" -tgtuser=sqladmin -tgtpass=pass123 -tgthost=inf-dev-db-0 -tgttable=scratchpad.company_1k

	SOURCE DATABASE FLAGS
	=====================
	-srcuser: Source Username (required)
	-srcpass: Source Password (interactive prompt if blank)
	-srchost: Source Database (localhost assumed if blank)
	-srcport: Source MySQL Port (3306 default)
	-srcsocket: Source MySQL Socket File
	-srctable: Fully Qualified Source Tablename: ex. schema.tablename (required)
	-where: Where clause to apply to source table select

	SOURCE DATABASE FLAGS
	=====================
	-tgtuser: Target Username (source username used if blank)
	-tgtpass: Target Password (source password used if blank)
	-tgthost: Target Database (required)
	-tgtport: Target MySQL Port (3306 default)
	-tgtsocket: Target MySQL Socket File
	-tgttable: Fully Qualified Target Tablename: ex. schema.tablename (source tablename used if blank)


	DEBUG FLAGS
	===========
	-debug_cpu: CPU debugging filename
	-debug_mem: Memory debugging filename
	-version: Version information

	`)
}

func main() {
	start := time.Now()

	// Catch signals
	catchNotifications()

	// Profiling flags
	cpuprofile := flag.String("debug_cpu", "", "CPU debugging filename")
	memprofile := flag.String("debug_mem", "", "Memory debugging filename")

	// Source flags
	fSrcUser := flag.String("srcuser", "", "Source Username (required)")
	fSrcPass := flag.String("srcpass", "", "Source Password (interactive prompt if blank)")
	fSrcHost := flag.String("srchost", "", "Source Database (localhost assumed if blank)")
	fSrcPort := flag.String("srcport", "3306", "Source MySQL Port")
	fSrcSock := flag.String("srcsocket", "", "Source MySQL Socket File")
	fSrcTable := flag.String("srctable", "", "Fully Qualified Source Tablename: ex. schema.tablename (required)")
	fSrcWhere := flag.String("where", "", "Where clause to apply to source table select")

	// Target flags
	fTgtUser := flag.String("tgtuser", "", "Target Username (source username used if blank)")
	fTgtPass := flag.String("tgtpass", "", "Target Password (source password used if blank)")
	fTgtHost := flag.String("tgthost", "", "Target Database (required)")
	fTgtPort := flag.String("tgtport", "3306", "Target MySQL Port")
	fTgtTable := flag.String("tgttable", "", "Fully Qualified Target Tablename: ex. schema.tablename (source tablename used if blank)")

	// Other flags
	version := flag.Bool("version", false, "Version information")
	help := flag.Bool("help", false, "Show usage")
	h := flag.Bool("h", false, "Show usage")

	flag.Parse()

	// Print usage
	if flag.NFlag() == 0 || *help == true || *h == true {
		showUsage()

		os.Exit(0)
	}

	if *version {
		fmt.Printf("mytablecopy version %s\n", versionInformation)
		os.Exit(0)
	}

	// CPU Profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		checkErr(err)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Default to localhost if no host or socket provided
	if *fSrcSock == "" && *fSrcHost == "" {
		*fSrcSock = "/var/lib/mysql/mysql.sock"
	}

	// Need to provide a target
	if *fTgtHost == "" {
		fmt.Println("You must provide a target database")
		os.Exit(1)
	}

	// A fully qualified table must be provided
	if *fTgtTable == "" && *fSrcTable != "" {
		*fTgtTable = *fSrcTable
	} else if *fSrcTable == "" || !strings.Contains(*fSrcTable, ".") {
		fmt.Println("You must provide a fully qualifed table to move")
		os.Exit(1)
	}

	// If password is blank prompt user - Not perfect as it prints the password typed to the screen
	if *fSrcPass == "" {
		fmt.Println("Enter password: ")
		//		pwd, err := readPassword(0)
		pwd, err := terminal.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			if err != io.EOF {
				checkErr(err)
			}
		}

		*fSrcPass = string(pwd)
	}

	// Use source username if target not supplied
	if *fTgtUser == "" && *fSrcUser != "" {
		*fTgtUser = *fSrcUser
	}

	// Use source password if target not supplied
	if *fTgtPass == "" && *fSrcPass != "" {
		*fTgtPass = *fSrcPass
	}

	// Add where keyword if where clause is supplied
	if *fSrcWhere != "" {
		*fSrcWhere = " where " + *fSrcWhere
	}

	// Split the table into schema and table name
	srcSplit := strings.Split(*fSrcTable, ".")
	tgtSplit := strings.Split(*fTgtTable, ".")

	source := dbInfo{user: *fSrcUser, pass: *fSrcPass, host: *fSrcHost, port: *fSrcPort, sock: *fSrcSock, schema: srcSplit[0], table: srcSplit[1], where: *fSrcWhere}
	target := dbInfo{user: *fTgtUser, pass: *fTgtPass, host: *fTgtHost, port: *fTgtPort, schema: tgtSplit[0], table: tgtSplit[1]}

	// Create a *sql.DB connection to the source database
	sourceDB, err := source.Connect()
	defer sourceDB.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	source.db = sourceDB

	// Create a *sql.DB connection to the target database
	targetDB, err := target.Connect()
	defer targetDB.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	target.db = targetDB

	createStmt := source.readTable()

	target.columns = source.getColumns()

	checkSchema(&source, &target)
	writeTable(&source, &target, createStmt)

	dataChan := make(chan []sql.RawBytes)
	quitChan := make(chan bool)
	goChan := make(chan bool)
	go readRows(&source, &target, dataChan, quitChan, goChan)
	target.writeRows(dataChan, goChan)

	<-quitChan
	close(quitChan)
	close(goChan)

	//  fmt.Println(source)
	//  fmt.Println(target)

	// Memory Profiling
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		checkErr(err)
		pprof.WriteHeapProfile(f)
		defer f.Close()
	}

	fmt.Println()
	fmt.Println("Total runtime =", time.Since(start))
}

// Pass the buck error catching
func checkErr(e error) {
	if e != nil {
		log.Panic(e)
	}
}

// Catch signals
func catchNotifications() {
	state, err := terminal.GetState(int(os.Stdin.Fd()))
	checkErr(err)

	// Deal with SIGINT
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	var timer time.Time
	go func() {
		for sig := range sigChan {
			// Prevent exiting on accidental signal send
			if time.Now().Sub(timer) < time.Second*signalTimeout {
				terminal.Restore(int(os.Stdin.Fd()), state)
				os.Exit(0)
			}

			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, sig, "signal caught!")
			fmt.Fprintf(os.Stderr, "Send signal again within %v seconds to exit\n", signalTimeout)
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "")

			timer = time.Now()
		}
	}()
}

// Create and return a database handle
func (dbi *dbInfo) Connect() (*sql.DB, error) {
	var db *sql.DB
	var err error
	if dbi.sock != "" {
		db, err = sql.Open("mysql", dbi.user+":"+dbi.pass+"@unix("+dbi.sock+")/?allowCleartextPasswords=1&tls=skip-verify")
		checkErr(err)
	} else if dbi.host != "" {
		db, err = sql.Open("mysql", dbi.user+":"+dbi.pass+"@tcp("+dbi.host+":"+dbi.port+")/?allowCleartextPasswords=1&tls=skip-verify")
	}

	// Ping database to verify credentials
	err = db.Ping()

	return db, err
}

// Adds backtick quotes in cases where identifiers are all numeric or match reserved keywords
func addQuotes(s string) string {
	s = "`" + s + "`"
	return s
}

func (src *dbInfo) readTable() string {
	var err error
	var ignore string
	var stmt string
	err = src.db.QueryRow("show create table "+addQuotes(src.schema)+"."+addQuotes(src.table)).Scan(&ignore, &stmt)
	checkErr(err)

	return stmt
}

func (src *dbInfo) getColumns() []string {
	var cols = []string{}
	rows, err := src.db.Query("select data_type from information_schema.columns where table_schema = '" + src.schema + "' and table_name = '" + src.table + "'")
	defer rows.Close()
	checkErr(err)

	var dataType string
	for rows.Next() {
		err = rows.Scan(&dataType)
		checkErr(err)

		cols = append(cols, dataType)
	}
	checkErr(err)

	return cols
}

// checkSchema creates a schema if it does not already exist
func checkSchema(src, tgt *dbInfo) {
	var exists string
	err := tgt.db.QueryRow("show databases like '" + tgt.schema + "'").Scan(&exists)

	if err != nil {
		var charSet string
		err := src.db.QueryRow("select default_character_set_name from information_schema.schemata where schema_name = '" + src.schema + "'").Scan(&charSet)

		_, err = tgt.db.Exec("create database " + addQuotes(tgt.schema) + " default character set " + charSet)
		checkErr(err)

		fmt.Println("       Created schema", tgt.schema)
	}
}

func writeTable(src, tgt *dbInfo, tableCreate string) {
	// Start db transaction
	tx, err := tgt.db.Begin()
	checkErr(err)

	_, err = tx.Exec("use " + tgt.schema)

	// Drop table if exists
	_, err = tx.Exec("drop table if exists " + addQuotes(tgt.table))
	checkErr(err)

	// Change table name if different
	if src.table != tgt.table {
		tableCreate = strings.Replace(tableCreate, src.table, tgt.table, 1)
	}

	// Create table
	_, err = tx.Exec(tableCreate)
	checkErr(err)

	// Commit transaction
	err = tx.Commit()
	checkErr(err)
}

func readRows(src, tgt *dbInfo, dataChan chan []sql.RawBytes, quitChan chan bool, goChan chan bool) {
	rows, err := src.db.Query("select * from " + addQuotes(src.schema) + "." + addQuotes(src.table) + src.where)
	defer rows.Close()
	checkErr(err)

	cols, err := rows.Columns()
	checkErr(err)

	// Need to scan into empty interface since we don't know how many columns or their types
	scanVals := make([]interface{}, len(cols))
	vals := make([]sql.RawBytes, len(cols))
	stray := make([]sql.RawBytes, len(cols))
	for i := range vals {
		scanVals[i] = &vals[i]
	}

	var rowNum int64
	for rows.Next() {
		err := rows.Scan(scanVals...)
		checkErr(err)

		copy(stray, vals)

		dataChan <- stray
		rowNum++

		// Block until writeRows() signals it is safe to proceed
		// This is necessary because sql.RawBytes is a memory pointer and rows.Next() will loop and change the memory address before writeRows can properly process the values
		<-goChan
	}

	err = rows.Err()
	checkErr(err)

	close(dataChan)
	quitChan <- true

	fmt.Println(rowNum, "rows inserted")
}

func (target *dbInfo) writeRows(dataChan chan []sql.RawBytes, goChan chan bool) {
	var cleaned []byte
	buf := bytes.NewBuffer(make([]byte, 0, insertBufferSize))

	sqlPrefix := "insert into " + addQuotes(target.schema) + "." + addQuotes(target.table) + " values ("
	prefix, _ := buf.WriteString(sqlPrefix)

	append := false
	for data := range dataChan {
		if append {
			buf.WriteString(",(")
		}
		append = true

		for i, col := range data {
			if col == nil {
				buf.WriteString("NULL")
			} else if len(col) == 0 {
				buf.WriteString("''")
			} else {
				switch target.columns[i] {
				case "tinytext":
					fallthrough
				case "text":
					fallthrough
				case "mediumtext":
					fallthrough
				case "longtext":
					fallthrough
				case "varchar":
					fallthrough
				case "char":
					cleaned = col
					cleaned = bytes.Replace(cleaned, []byte(`\`), []byte(`\\`), -1)
					cleaned = bytes.Replace(cleaned, []byte(`'`), []byte(`\'`), -1)
					buf.WriteString("'")
					buf.Write(cleaned)
					buf.WriteString("'")
				default:
					buf.WriteString("'")
					buf.Write(col)
					buf.WriteString("'")
				}
			}

			if i < len(target.columns)-1 {
				buf.WriteString(",")
			}
		}

		buf.WriteString(")")

		if buf.Len() > insertBufferSize {
			//buf.WriteTo(os.Stdout) // DEBUG
			//fmt.Println()          // DEBUG
			_, err := target.db.Exec(buf.String())
			if err != nil {
				fmt.Println(buf.String())
			}
			checkErr(err)

			buf.Reset()
			buf.WriteString(sqlPrefix)
			append = false
		}

		// Allow read function to loop over rows
		goChan <- true
	}

	// Apply left over data
	if buf.Len() > prefix {
		//buf.WriteTo(os.Stdout) // DEBUG
		//fmt.Println()          // DEBUG
		_, err := target.db.Exec(buf.String())
		checkErr(err)
	}
}
