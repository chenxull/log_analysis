package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client/v2"
)

type Reader interface {
	Read(readChan chan []byte)
}
type Writer interface {
	Write(writeChan chan *LogMessage)
}

type ReadFromFile struct {
	path string
	fd   *os.File
}

//WriteToInfluxDB 写入数据库中的信息
type WriteToInfluxDB struct {
	batch       uint16
	retry       uint8
	influxsConf *InfluConf
}

//InfluConf 连接数据库的参数
type InfluConf struct {
	Addr      string
	Username  string
	Password  string
	Database  string
	Precision string
}

// LogProcess 日志处理结构体
type LogProcess struct {
	readChan  chan []byte
	writeChan chan *LogMessage
	read      Reader
	write     Writer
}

//LogMessage 日志信息结构体
type LogMessage struct {
	TimeLocal                    time.Time
	ByteSent                     int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

//系统监控数据
type SystemInfo struct {
	HandleLine   int     `json:"handleLine"` //总处理日志行数
	Tps          float64 `json:"tps"`        //系统吐出量
	ReadChanLen  int     `json:"readChanLen"`
	WriteChanLen int     `json:"writeChanLen"`
	RunTime      string  `json:"runTime"`
	ErrNum       int     `json:"errNum"`
}

//监控结构体
type Monitor struct {
	listenPort string
	startTime  time.Time
	tpsSli     []int
	systemInfo SystemInfo
}

const (
	TypeHandleLine = 0
	TypeErrNum     = 1
)

var (
	path, influxDsn, listenPort string
	processNum, writeNum        int
	TypeMonitorChan             = make(chan int, 200)
)

//通过 http 的方式将监控信息暴露出去
func (m *Monitor) start(lp *LogProcess) {
	// 使用 channel 作为中转，收集各个 goroutine 中的数据
	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeErrNum:
				m.systemInfo.ErrNum++
			case TypeHandleLine:
				m.systemInfo.HandleLine++
			}
		}
	}()

	//创建定时器，每隔5秒计算tps
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			m.tpsSli = append(m.tpsSli, m.systemInfo.HandleLine)
			if len(m.tpsSli) > 2 {
				m.tpsSli = m.tpsSli[1:]
			}
		}
	}()

	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		io.WriteString(writer, m.SystemStatus(lp))
	})

	log.Fatal(http.ListenAndServe(":"+m.listenPort, nil))
}

// SystemStatus 获取系统监控信息
func (m *Monitor) SystemStatus(lp *LogProcess) string {
	m.systemInfo.RunTime = time.Now().Sub(m.startTime).String()
	m.systemInfo.ReadChanLen = len(lp.readChan)
	m.systemInfo.WriteChanLen = len(lp.writeChan)

	if len(m.tpsSli) >= 2 {
		m.systemInfo.Tps = float64(m.tpsSli[1]-m.tpsSli[0]) / 5
	}

	ret, _ := json.MarshalIndent(m.systemInfo, "", "\t")
	return string(ret)
}

//Process 处理日志文件
func (l *LogProcess) Process() {

	/*
		'$remote_addr\t$http_x_forwarded_for\t$remote_user\t[$time_local]\t$scheme\t"$request"\t$status\t$body_bytes_sent\t"$http_referer"\t"$http_user_agent"\t"$gzip_ratio"\t$upstream_response_time\t$request_time'
	*/

	//1.处理日志信息的正则表达式
	r := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	// 获取时间
	loc, _ := time.LoadLocation("Asia/Shanghai")

	for data := range l.readChan {
		//fmt.Println("DEBUG::", data)
		ret := r.FindStringSubmatch(string(data))
		if len(ret) != 14 {
			TypeMonitorChan <- TypeErrNum
			log.Println("FindStrngsSubmatch fail:", string(data))
			continue
		}

		timeLocal, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
			continue
		}
		//日志流量

		// 获取请求类型
		reqSli := strings.Split(ret[6], " ")
		if len(reqSli) != 3 {
			TypeMonitorChan <- TypeErrNum
			log.Println("strings.split fail", ret[6])
			continue
		}
		method := reqSli[0]

		// 解析 url
		u, err := url.Parse(reqSli[1])
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("url parse fail", err)
			continue
		}
		path := u.Path
		scheme := ret[5]
		status := ret[7]
		byteSent, _ := strconv.Atoi(ret[8])
		upstringTime, _ := strconv.ParseFloat(ret[12], 64)
		requestTime, _ := strconv.ParseFloat(ret[13], 64)

		l.writeChan <- &LogMessage{
			TimeLocal:    timeLocal,
			ByteSent:     byteSent,
			Method:       method,
			Path:         path,
			Scheme:       scheme,
			Status:       status,
			UpstreamTime: upstringTime,
			RequestTime:  requestTime,
		}
	}

}

func (r *ReadFromFile) Read(readChan chan []byte) {
	defer close(readChan)

	//2.从文件末尾一行一行读取
	r.fd.Seek(0, 2)

	rd := bufio.NewReader(r.fd)
	for {
		line, err := rd.ReadBytes('\n')
		//fmt.Println("debug::读取数据", string(line))
		if err == io.EOF {
			time.Sleep(500 * time.Millisecond)
			continue
		} else if err != nil {
			TypeMonitorChan <- TypeErrNum
			panic(fmt.Sprintf("ReadBytes error:%s", err.Error()))
		}
		TypeMonitorChan <- TypeHandleLine
		readChan <- line[:len(line)-1]
	}

	//

}

func (w *WriteToInfluxDB) Write(writeChan chan *LogMessage) {

	//初始化influxdb client
	infclient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     w.influxsConf.Addr,
		Username: w.influxsConf.Username,
		Password: w.influxsConf.Password,
	})

	if err != nil {
		log.Fatal(err)
	}

	//构造数据写入 influxdb
	for {

		//create a new point batch
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  w.influxsConf.Database,
			Precision: w.influxsConf.Precision,
		})
		//fmt.Println("debug")
		if err != nil {
			log.Fatal(err)
		}

		// 统计日志数量
		var count uint16
	Fetch:
		for data := range writeChan {
			//create a point and add to batch
			//tags:Path,Method,Scheme,Status
			tags := map[string]string{
				"Path":   data.Path,
				"Method": data.Method,
				"Scheme": data.Scheme,
				"Status": data.Status,
			}

			//fields:UpstreamTime,RequestTime,ByteSent
			fields := map[string]interface{}{
				"UpstreamTime": data.UpstreamTime,
				"RequestTime":  data.RequestTime,
				"ByteSent":     data.ByteSent,
			}

			pt, err := client.NewPoint("nginx_log", tags, fields, data.TimeLocal)
			if err != nil {
				TypeMonitorChan <- TypeErrNum
				log.Fatal(err)
				continue
			}

			bp.AddPoint(pt)
			count++
			//fmt.Println("debug::", count)
			//当日志数量大于50时才发送
			if count > w.batch {
				break Fetch
			}
		}
		var i uint8
		for i = 1; i < w.retry; i++ {
			if err := infclient.Write(bp); err != nil {
				TypeMonitorChan <- TypeErrNum
			} else {
				log.Println(w.batch, "point has writen")
				break
			}
		}

	}

}

//NewReader 构建 reader
func NewReader(path string) (Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &ReadFromFile{
		fd:   f,
		path: path,
	}, nil
}

// NewWriter 工厂方法创建 writer
func NewWriter(influxDsn string) (Writer, error) {
	// influxDsn: http://ip:port@username@password@db@precision
	influxSli := strings.Split(influxDsn, "@")
	if len(influxSli) < 5 {
		return nil, errors.New("param influxDns err")
	}

	return &WriteToInfluxDB{
		batch: 50,
		retry: 3,
		influxsConf: &InfluConf{
			Addr:      influxSli[0],
			Username:  influxSli[1],
			Password:  influxSli[2],
			Database:  influxSli[3],
			Precision: influxSli[4],
		},
	}, nil

}

// NewLogProcess 工厂方法初始化处理器
func NewLogProcess(reader Reader, writer Writer) *LogProcess {

	return &LogProcess{
		readChan:  make(chan []byte, 200),
		writeChan: make(chan *LogMessage, 200),
		read:      reader,
		write:     writer,
	}
}

func init() {
	flag.StringVar(&listenPort, "listenport", "9193", "monitor port")
	flag.StringVar(&path, "path", "./access.log", "read file path")
	flag.StringVar(&influxDsn, "influxDsn", "Http://127.0.0.1:8086@chenxu@chenxupass@loginfo@s", "data source")
	flag.IntVar(&processNum, "processNum", 2, "process goroutine num")
	flag.IntVar(&writeNum, "writeNum", 3, "write goroutine num")
	flag.Parse()
}

func main() {

	reader, err := NewReader(path)
	if err != nil {
		panic(err)
	}

	writer, err := NewWriter(influxDsn)
	if err != nil {
		panic(err)
	}

	lp := NewLogProcess(reader, writer)

	go lp.read.Read(lp.readChan)

	fmt.Println(processNum)

	for i := 0; i < processNum; i++ {
		go lp.Process()
	}

	fmt.Println(writeNum)
	for i := 0; i < writeNum; i++ {
		go lp.write.Write(lp.writeChan)
	}

	m := &Monitor{
		startTime:  time.Now(),
		systemInfo: SystemInfo{},
	}

	go m.start(lp)
	time.Sleep(300 * time.Second)
}
