package master

import (
	"errors"
	"log"
	"strings"

	client "github.com/influxdata/influxdb1-client/v2"
)

type Writer interface {
	Write(writeChan chan *LogMessage)
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

var (
	GlobalWrite *WriteToInfluxDB
)

// NewWriter 工厂方法创建 writer
func InitWriter(influxDsn string) error {
	// influxDsn: http://ip:port@username@password@db@precision
	influxSli := strings.Split(influxDsn, "@")
	
	if len(influxSli) < 5 {
		return errors.New("param influxDns err")
	}

	GlobalWrite = &WriteToInfluxDB{
		batch: 50,
		retry: 3,
		influxsConf: &InfluConf{
			Addr:      influxSli[0],
			Username:  influxSli[1],
			Password:  influxSli[2],
			Database:  influxSli[3],
			Precision: influxSli[4],
		},
	}
	return nil

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
