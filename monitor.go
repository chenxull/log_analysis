package master

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

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
	ListenPort string
	StartTime  time.Time
	TpsSli     []int
	SysInfo    SystemInfo
}

const (
	TypeHandleLine = 0
	TypeErrNum     = 1
)

var (
	TypeMonitorChan = make(chan int, 200)
)

var (
	GlobalMonitor *Monitor
)

func Initmonitor() error {

	GlobalMonitor = &Monitor{
		StartTime: time.Now(),
		SysInfo:   SystemInfo{},
	}
	return nil
}

//通过 http 的方式将监控信息暴露出去
func (m *Monitor) Start(lp *LogProcess) {
	// 使用 channel 作为中转，收集各个 goroutine 中的数据
	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeErrNum:
				m.SysInfo.ErrNum++
			case TypeHandleLine:
				m.SysInfo.HandleLine++
			}
		}
	}()

	//创建定时器，每隔5秒计算tps
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			m.TpsSli = append(m.TpsSli, m.SysInfo.HandleLine)
			if len(m.TpsSli) > 2 {
				m.TpsSli = m.TpsSli[1:]
			}
		}
	}()

	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		io.WriteString(writer, m.SystemStatus(lp))
	})

	log.Fatal(http.ListenAndServe(":"+m.ListenPort, nil))
}

// SystemStatus 获取系统监控信息
func (m *Monitor) SystemStatus(lp *LogProcess) string {
	m.SysInfo.RunTime = time.Now().Sub(m.StartTime).String()
	m.SysInfo.ReadChanLen = len(lp.ReadChan)
	m.SysInfo.WriteChanLen = len(lp.WriteChan)

	if len(m.TpsSli) >= 2 {
		m.SysInfo.Tps = float64(m.TpsSli[1]-m.TpsSli[0]) / 5
	}

	ret, _ := json.MarshalIndent(m.SysInfo, "", "\t")
	return string(ret)
}
