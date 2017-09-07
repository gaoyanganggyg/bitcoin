package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type K_Line struct {
	tm            string
	open          float64
	high          float64
	low           float64
	cl            float64
	amount        float64
	tm_stmp       int64
	create_tm     string
	create_tmstmp int64
}

func parseData(data []byte) (interface{}, error) {
	var res []interface{}
	err := json.Unmarshal(data, &res)
	if nil != err {
		return nil, err
	}
	return res, nil
}

var (
	db      *sql.DB
	db_user string = "root"
	db_pwd  string = ""
	tmNow   time.Time
)

func InsertDB(k *K_Line) {
	_, err := db.Exec("INSERT INTO k_line_1_min(time, start, high, low, close, amount, tm_stmp, c_tm, c_tm_stmp) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
		k.tm, k.open, k.high, k.low, k.cl, k.amount, k.tm_stmp, k.create_tm, k.create_tmstmp)

	if nil != err {
		log.Fatal("insert to talbe error: " + err.Error())
	}
}

func printData(d *interface{}) {
	for _, d1 := range (*d).([]interface{}) {
		d2 := d1.([]interface{})

		tm := d2[0].(string)
		open := d2[1].(float64)
		high := d2[2].(float64)
		low := d2[3].(float64)
		close := d2[4].(float64)
		amount := d2[5].(float64)

		tmF := fmt.Sprintf("%s-%s-%s %s:%s:%s", tm[0:4], tm[4:6], tm[6:8], tm[8:10], tm[10:12], tm[12:14])

		loc, _ := time.LoadLocation("Local")
		tmStmp, err := time.ParseInLocation("20060102150400000", tm, loc)
		if err != nil {
			log.Println("time parse error: " + err.Error())
			return
		}
		tmNowStr := tmNow.Format("2006-01-02 15:04:05")

		k_line := K_Line{tmF, open, high, low, close, amount, tmStmp.Unix(), tmNowStr, tmNow.Unix()}

		InsertDB(&k_line)
	}

}

func get(url string) (data []byte, err error) {
	resp, err := http.Get(url)
	if nil != err {
		return nil, err
	}
	if 200 != resp.StatusCode {
		return nil, errors.New("status code: " + string(resp.StatusCode))
	}
	data1, err1 := ioutil.ReadAll(resp.Body)
	if nil != err1 {
		return nil, errors.New("read resp body error: " + err1.Error())
	}
	return data1, nil
}

func main() {
	tmp, err := sql.Open("mysql", fmt.Sprintf("%s:%s@/bitcoin?charset=utf8", db_user, db_pwd))
	if nil != err {
		log.Fatalln("open sql error : " + err.Error())
	}
	db = tmp

	t := time.Tick(time.Minute * 1)
	for range t {
		tmNow = time.Now()

		data, err := get("http://api.huobi.com/staticmarket/btc_kline_001_json.js?length=1")
		if nil != err {
			log.Fatalln("request error: " + err.Error())
		}

		data1, err1 := parseData(data)
		if nil != err1 {
			log.Fatalln("parse resp error: " + err1.Error())
		}

		printData(&data1)
	}

}
