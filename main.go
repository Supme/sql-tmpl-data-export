package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"os"
	"path"
	"strings"
	"text/template"
)

const AppVersion = "0.0.1 alpha"

var config struct {
	SqlType            string `toml:"sql_type"`
	ConnectString      string `toml:"connect_string"`
	SqlFilePath        string `toml:"sql_file"`
	TmplHeaderFilePath string `toml:"header_tmpl_file"`
	TmplRowFilePath    string `toml:"row_tmpl_file"`
}

var (
	configFilePath string
)

func init() {
	flag.StringVar(&configFilePath, "c", "config.toml", "Config file")
	version := flag.Bool("v", false, "Prints version")
	flag.Parse()
	if *version {
		fmt.Printf("SQL Template Data Export version: v%s\r\n\r\n", AppVersion)
		os.Exit(0)
	}
}

func main() {
	// Выводить пока будем в stdout
	out := os.Stdout

	// Читаем конфиг
	if _, err := toml.DecodeFile(configFilePath, &config); err != nil {
		log.Fatalf("Open config file: %s", err)
	}

	// Читаем SQL запрос из файла
	sqlRequestFile, err := os.Open(config.SqlFilePath)
	if err != nil {
		log.Fatalf("Open sql file: %s", err)
	}

	sqlRequest := &bytes.Buffer{}
	_, err = sqlRequest.ReadFrom(sqlRequestFile)
	if err != nil {
		log.Fatalf("Read sql file: %s", err)
	}

	err = sqlRequestFile.Close()
	if err != nil {
		log.Printf("Close sql file: %s", err)
	}

	// Какие функции передаём в шаблоны
	funcMap := template.FuncMap{
		"strDoubleQuoted": strDoubleQuoted,
		"byteJoin":        byteJoin,
		"strJoin":         strJoin,
		"strToByte":       strToByte,
		"byteToStr":       byteToStr,
		"base64enc":       base64enc,
		"md5byte":         md5byte,
		"sha1byte":        sha1byte,
		"sha256byte":      sha256byte,
	}
	// Вычитываем шаблон с передачей функций
	hTmpl, err := template.New(path.Base(config.TmplHeaderFilePath)).Funcs(funcMap).ParseFiles(config.TmplHeaderFilePath)
	if err != nil {
		log.Fatalf("Open header template file: %s", err)
	}
	rTmpl, err := template.New(path.Base(config.TmplRowFilePath)).Funcs(funcMap).ParseFiles(config.TmplRowFilePath)
	if err != nil {
		log.Fatalf("Open row template file: %s", err)
	}
	// Создаём коннект с базой
	db, err := sqlx.Open(config.SqlType, config.ConnectString)
	if err != nil {
		log.Fatalf("Connect to db: %s", err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("Close database connect: %s", err)
		}
	}()
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)

	res, err := db.Queryx(sqlRequest.String())
	if err != nil {
		log.Fatalf("SQL request: %s", err)
	}
	defer func() {
		err := res.Close()
		if err != nil {
			log.Printf("Query close: %s", err)
		}
	}()

	columns, err := res.Columns()
	if err != nil {
		log.Printf("Get columns name from result: %s", err)
	}

	err = hTmpl.Execute(out, columns)
	if err != nil {
		log.Printf("Header template: %s", err)
	}

	i := 0
	for res.Next() {
		i++

		r := map[string]interface{}{}
		err = res.MapScan(r)
		if err != nil {
			log.Printf("Scan row %d result: %s", i, err)
		}

		err = rTmpl.Execute(out, r)
		if err != nil {
			log.Printf("Row %d template: %s", i, err)
		}
	}
}

func strDoubleQuoted(s string) string {
	return strings.ReplaceAll(s, `"`, `""`)
}

func strJoin(s ...string) string {
	return strings.Join(s, "")
}

func byteJoin(b ...[]byte) []byte {
	bs := &bytes.Buffer{}
	for i := range b {
		bs.Write(b[i])
	}
	return bs.Bytes()
}

func byteToStr(b []byte) string {
	return string(b)
}

func strToByte(s string) []byte {
	return []byte(s)
}

func base64enc(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func md5byte(b []byte) []byte {
	h := md5.New()
	h.Write(b)
	return h.Sum(nil)
}

func sha1byte(b []byte) []byte {
	h := sha1.New()
	h.Write(b)
	return h.Sum(nil)
}

func sha256byte(b []byte) []byte {
	h := sha256.New()
	h.Write(b)
	return h.Sum(nil)
}
