package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
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

type Tick struct {
	Open float64
	Last float64
	Low  float64
	High float64
	Vol  float64
	Buy  float64
	Sell float64
}

type StaticMarket struct {
	Time   string
	Ticker Tick
}

var (
	db      *sql.DB
	db_user string = ""
	db_pwd  string = "!"
	db_url  string = ""
	db_port int    = 0
	tmNow   time.Time
	lastTm  string = ""
)

func parseData(data []byte) (interface{}, error) {
	var res []interface{}
	err := json.Unmarshal(data, &res)
	if nil != err {
		return nil, err
	}
	return res, nil
}

func ParseStaticMarket(data []byte) (StaticMarket, error) {
	var staticMarket StaticMarket
	err := json.Unmarshal(data, &staticMarket)
	if nil != err {
		return staticMarket, err
	}
	return staticMarket, nil
}

func InsertDB(k *K_Line) {
	_, err := db.Exec("INSERT INTO k_line_1_min(time, start, high, low, close, amount, tm_stmp, c_tm, c_tm_stmp)  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
		k.tm, k.open, k.high, k.low, k.cl, k.amount, k.tm_stmp, k.create_tm, k.create_tmstmp)

	if nil != err {
		log.Println("insert to talbe error: " + err.Error())
		return
	}
}

func InsertStaticMarket(s *StaticMarket) {
	tm_int, _ := strconv.ParseInt(s.Time, 10, 32)

	tm_str := time.Unix(tm_int, 0).Format("2006-01-02 15:04:05")
	tm_cr_stmp := tmNow.Unix()
	tm_cr_str := tmNow.Format("2006-01-02 15:04:05")

	_, err := db.Exec("INSERT INTO static_market(tm_stmp, tm_str, tm_cr_stmp, tm_cr_str, open, last, low, high, vol, buy, sell) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		s.Time, tm_str, tm_cr_stmp, tm_cr_str, s.Ticker.Open, s.Ticker.Last, s.Ticker.Low, s.Ticker.High, s.Ticker.Vol, s.Ticker.Buy, s.Ticker.Sell)

	if nil != err {
		log.Println("insert to talbe error: " + err.Error())
		return
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

func KLine() {
	tmNow = time.Now()

	data, err := get("http://api.huobi.com/staticmarket/btc_kline_001_json.js?length=1")
	if nil != err {
		log.Println("request error: " + err.Error())
		return
	}

	data1, err1 := parseData(data)
	if nil != err1 {
		log.Println("parse resp error: " + err1.Error())
		return
	}
	printData(&data1)
}

func StaticMarketF() {
	tmNow = time.Now()

	data, err := get("http://api.huobi.com/staticmarket/ticker_btc_json.js")
	if nil != err {
		log.Println("request error: " + err.Error())
		return
	}
	res, err1 := ParseStaticMarket(data)
	if nil != err1 {
		log.Println("parse error: " + err1.Error())
		return
	}
	if lastTm != res.Time {
		lastTm = res.Time
		InsertStaticMarket(&res)
	}
}

func Ticker(dur time.Duration, fun func()) {
	t := time.Tick(dur)
	for range t {
		fun()
	}
}

func main() {
	tmp, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/bitcoin?charset=utf8", db_user, db_pwd, db_url, db_port))
	if nil != err {
		log.Fatalln("open sql error : " + err.Error())
	}
	db = tmp

	val := flag.Uint("t", 0, "0 => k_line, 1 => static market")
	flag.Parse()

	switch *val {
	case 0:
		Ticker(time.Minute, KLine)
	case 1:
		Ticker(time.Second*2, StaticMarketF)
	default:
		fmt.Println("nothing")
	}

}
