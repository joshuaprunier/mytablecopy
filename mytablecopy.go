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
	"syscall"
	"time"
	"unsafe"

	_ "github.com/go-sql-driver/mysql" // Go MySQL driver
)

const insertBufferSize = 1048576

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

func main() {
	start := time.Now()

	// Trap for SIGINT, may need to trap other signals in the future as well
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Syscall in case signal is sent during terminal echo off
	var oldState syscall.Termios
	syscall.Syscall6(syscall.SYS_IOCTL, uintptr(0), syscall.TCGETS, uintptr(unsafe.Pointer(&oldState)), 0, 0, 0)

	var timer time.Time
	go func() {
		for sig := range sigChan {
			if time.Now().Sub(timer) < time.Second*5 {
				syscall.Syscall6(syscall.SYS_IOCTL, uintptr(0), syscall.TCSETS, uintptr(unsafe.Pointer(&oldState)), 0, 0, 0)
				os.Exit(0)
			}

			fmt.Println()
			fmt.Println(sig, "signal caught!")
			fmt.Println("Send signal again within 3 seconds to exit")

			timer = time.Now()
		}
	}()

	// Profiling flags
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")

	// Source flags
	fSrcUser := flag.String("srcuser", "", "Source Username")
	fSrcPass := flag.String("srcpassword", "", "Source Password")
	fSrcHost := flag.String("srchost", "", "Source Database")
	fSrcPort := flag.String("srcport", "3306", "Source MySQL Port")
	fSrcSock := flag.String("srcsocket", "", "Source MySQL Socket")
	fSrcTable := flag.String("srctable", "", "Source Table (must be fully qualified ex. schema.tablename)")
	fSrcWhere := flag.String("where", "", "Where clause to apply to source table select")

	// Target flags
	fTgtUser := flag.String("tgtuser", "", "Target Username")
	fTgtPass := flag.String("tgtpassword", "", "Target Password")
	fTgtHost := flag.String("tgthost", "", "Target Database")
	fTgtPort := flag.String("tgtport", "3306", "Target MySQL Port")
	fTgtTable := flag.String("tgttable", "", "Target Table (must be fully qualified ex. schema.tablename)")

	flag.Parse()

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
		pwd, err := readPassword(0)
		checkErr(err)
		*fSrcPass = string(pwd)
	}

	if *fTgtUser == "" && *fSrcUser != "" {
		*fTgtUser = *fSrcUser
	}

	if *fTgtPass == "" && *fSrcPass != "" {
		*fTgtPass = *fSrcPass
	}

	// Split the table into schema and table name
	srcSplit := strings.Split(*fSrcTable, ".")
	tgtSplit := strings.Split(*fTgtTable, ".")

	source := dbInfo{user: *fSrcUser, pass: *fSrcPass, host: *fSrcHost, port: *fSrcPort, sock: *fSrcSock, schema: srcSplit[0], table: srcSplit[1], where: *fSrcWhere}
	target := dbInfo{user: *fTgtUser, pass: *fTgtPass, host: *fTgtHost, port: *fTgtPort, schema: tgtSplit[0], table: tgtSplit[1]}

	// Create a *sql.DB connection to the source database
	sourceDB, err := source.dbConn()
	defer sourceDB.Close()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	source.db = sourceDB

	// Create a *sql.DB connection to the target database
	targetDB, err := target.dbConn()
	defer targetDB.Close()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	target.db = targetDB

	createStmt := source.readTable()

	target.columns = source.getColumns()

	checkSchema(&source, &target)
	target.writeTable(createStmt)

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

// readPassword is borrowed from the crypto/ssh/terminal sub repo to accept a password from stdin without local echo.
// http://godoc.org/code.google.com/p/go.crypto/ssh/terminal#Terminal.ReadPassword
func readPassword(fd int) ([]byte, error) {
	var oldState syscall.Termios
	if _, _, err := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), syscall.TCGETS, uintptr(unsafe.Pointer(&oldState)), 0, 0, 0); err != 0 {
		return nil, err
	}

	newState := oldState
	newState.Lflag &^= syscall.ECHO
	newState.Lflag |= syscall.ICANON | syscall.ISIG
	newState.Iflag |= syscall.ICRNL
	if _, _, err := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&newState)), 0, 0, 0); err != 0 {
		return nil, err
	}

	defer func() {
		syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&oldState)), 0, 0, 0)
	}()

	var buf [16]byte
	var ret []byte
	for {
		n, err := syscall.Read(fd, buf[:])
		if err != nil {
			return nil, err
		}

		if n == 0 {
			if len(ret) == 0 {
				return nil, io.EOF
			}
			break
		}

		if buf[n-1] == '\n' {
			n--
		}

		ret = append(ret, buf[:n]...)
		if n < len(buf) {
			break
		}
	}

	return ret, nil
}

func (d *dbInfo) dbConn() (*sql.DB, error) {
	// Determine tcp or socket connection
	var db *sql.DB
	var err error
	if d.sock != "" {
		db, err = sql.Open("mysql", d.user+":"+d.pass+"@unix("+d.sock+")/?allowCleartextPasswords=1")
		checkErr(err)
	} else if d.host != "" {
		db, err = sql.Open("mysql", d.user+":"+d.pass+"@tcp("+d.host+":"+d.port+")/?allowCleartextPasswords=1")
		checkErr(err)
	} else {
		fmt.Println("should be no else")
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

func (target *dbInfo) writeTable(tableCreate string) {
	// Start db transaction
	tx, err := target.db.Begin()
	checkErr(err)

	_, err = tx.Exec("use " + target.schema)

	// Drop table if exists
	_, err = tx.Exec("drop table if exists " + addQuotes(target.table))
	checkErr(err)

	// Create table
	_, err = tx.Exec(tableCreate)
	checkErr(err)

	// Commit transaction
	err = tx.Commit()
	checkErr(err)

}

func readRows(src, tgt *dbInfo, dataChan chan []sql.RawBytes, quitChan chan bool, goChan chan bool) {
	rows, err := src.db.Query("select * from " + addQuotes(src.schema) + "." + addQuotes(src.table) + " where " + src.where)
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

		//	for i, col := range vals {
		//		stray[i] = col
		//	}
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
