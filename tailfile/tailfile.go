package tailfile

import (
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
)

var (
	TailObj *tail.Tail
)

// tail 相关
func Init(filename string) (err error) {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	TailObj, err = tail.TailFile(filename, config)
	if err != nil {
		logrus.Error("tailfile: create tailobj for path:%s failed, err :%v\n", filename, err)
		return
	}
	return
}
