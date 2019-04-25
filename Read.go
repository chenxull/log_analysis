package master

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"
)

type Reader interface {
	Read(readChan chan []byte)
}

type ReadFromFile struct {
	path string
	fd   *os.File
}

var (
	GlobalRead *ReadFromFile
)

//NewReader 构建 reader
func InitReader(path string) error {
	fmt.Println("debug")
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	GlobalRead = &ReadFromFile{
		fd:   f,
		path: path,
	}
	return nil
}

func (r *ReadFromFile) Read(readChan chan []byte) {
	defer close(readChan)

	//2.从文件末尾一行一行读取
	r.fd.Seek(0, 2)

	rd := bufio.NewReader(r.fd)
	for {
		line, err := rd.ReadBytes('\n')
		fmt.Println("debug::读取数据", string(line))
		if err == io.EOF {
			time.Sleep(500 * time.Millisecond)
			continue
		} else if err != nil {

			//	TypeMonitorChan <- TypeErrNum
			panic(fmt.Sprintf("ReadBytes error:%s", err.Error()))
		}
		//	TypeMonitorChan <- TypeHandleLine
		readChan <- line[:len(line)-1]
	}

}
