package main

import (
	"flag"
	"time"

	"github.com/chenxull/logAnalyis"
)

var (
	Path, InfluxDsn, ListenPort string
	ProcessNum, WriteNum        int
	//TypeMonitorChan             = make(chan int, 200)
)

func init() {
	flag.StringVar(&ListenPort, "listenport", "9193", "monitor port")
	flag.StringVar(&Path, "path", "./access.log", "read file path")
	flag.StringVar(&InfluxDsn, "influxDsn", "Http://127.0.0.1:8086@chenxu@chenxupass@loginfo@s", "data source")
	flag.IntVar(&ProcessNum, "processNum", 2, "process goroutine num")
	flag.IntVar(&WriteNum, "writeNum", 3, "write goroutine num")
	flag.Parse()
}

func main() {

	err := master.InitReader(Path)
	if err != nil {
		panic(err)
	}
	err = master.InitWriter(InfluxDsn)
	if err != nil {
		panic(err)
	}

	master.InitLogProcess(master.GlobalRead, master.GlobalWrite)

	go master.GlobalRead.Read(master.GlobalLogProcess.ReadChan)

	for i := 0; i < ProcessNum; i++ {
		go master.GlobalLogProcess.Process()
	}

	for i := 0; i < WriteNum; i++ {
		go master.GlobalLogProcess.Write.Write(master.GlobalLogProcess.WriteChan)
	}

	master.Initmonitor()

	go master.GlobalMonitor.Start(master.GlobalLogProcess)

	time.Sleep(300 * time.Second)
}
