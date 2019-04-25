package master

import (
	"log"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// LogProcess 日志处理结构体
type LogProcess struct {
	ReadChan  chan []byte
	WriteChan chan *LogMessage
	Read      Reader
	Write     Writer
}

//LogMessage 日志信息结构体
type LogMessage struct {
	TimeLocal                    time.Time
	ByteSent                     int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

var (
	GlobalLogProcess *LogProcess
)

// NewLogProcess 工厂方法初始化处理器
func InitLogProcess(reader Reader, writer Writer) {

	GlobalLogProcess = &LogProcess{
		ReadChan:  make(chan []byte, 200),
		WriteChan: make(chan *LogMessage, 200),
		Read:      reader,
		Write:     writer,
	}
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

	for data := range l.ReadChan {
		//fmt.Println("DEBUG::", data)
		ret := r.FindStringSubmatch(string(data))
		if len(ret) != 14 {
			//TypeMonitorChan <- TypeErrNum
			log.Println("FindStrngsSubmatch fail:", string(data))
			continue
		}

		timeLocal, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			//TypeMonitorChan <- TypeErrNum
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
			continue
		}
		//日志流量

		// 获取请求类型
		reqSli := strings.Split(ret[6], " ")
		if len(reqSli) != 3 {
			//TypeMonitorChan <- TypeErrNum
			log.Println("strings.split fail", ret[6])
			continue
		}
		method := reqSli[0]

		// 解析 url
		u, err := url.Parse(reqSli[1])
		if err != nil {
			//TypeMonitorChan <- TypeErrNum
			log.Println("url parse fail", err)
			continue
		}
		path := u.Path
		scheme := ret[5]
		status := ret[7]
		byteSent, _ := strconv.Atoi(ret[8])
		upstringTime, _ := strconv.ParseFloat(ret[12], 64)
		requestTime, _ := strconv.ParseFloat(ret[13], 64)

		l.WriteChan <- &LogMessage{
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
