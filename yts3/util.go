package yts3

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

func parseClampedInt(in string, defaultValue, min, max int64) (int64, error) {
	var v int64
	if in == "" {
		v = defaultValue
	} else {
		var err error
		v, err = strconv.ParseInt(in, 10, 0)
		if err != nil {
			return defaultValue, ErrInvalidArgument
		}
	}

	if v < min {
		v = min
	} else if v > max {
		v = max
	}

	return v, nil
}

func ReadAll(r io.Reader, size int64) (b []byte, err error) {
	var n int
	b = make([]byte, size)
	n, err = io.ReadFull(r, b)
	if err == io.ErrUnexpectedEOF {
		return nil, ErrIncompleteBody
	} else if err != nil {
		return nil, err
	}

	if n != int(size) {
		return nil, ErrIncompleteBody
	}

	if extra, err := ioutil.ReadAll(r); err != nil {
		return nil, err
	} else if len(extra) > 0 {
		return nil, ErrIncompleteBody
	}

	return b, nil
}

/**字符串->时间对象*/
func Str2Time(formatTimeStr string) time.Time {
	timeLayout := "2006-01-02 15:04:05"
	loc, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation(timeLayout, formatTimeStr, loc) //使用模板在对应时区转化为time.time类型

	return theTime

}

/**字符串->时间戳*/
func Str2Stamp(formatTimeStr string) int64 {
	timeStruct := Str2Time(formatTimeStr)
	millisecond := timeStruct.UnixNano() / 1e6
	return millisecond
}

/**时间对象->字符串*/
func Time2Str() string {
	const shortForm = "2006-01-01 15:04:05"
	t := time.Now()
	temp := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
	str := temp.Format(shortForm)
	return str
}

/*时间对象->时间戳*/
func Time2Stamp() int64 {
	t := time.Now()
	millisecond := t.UnixNano() / 1e6
	return millisecond
}

/*时间戳->字符串*/
func Stamp2Str(stamp int64) string {
	timeLayout := "2006-01-02 15:04:05"
	str := time.Unix(stamp/1000, 0).Format(timeLayout)
	return str
}

/*时间戳->时间对象*/
func Stamp2Time(stamp int64) time.Time {
	stampStr := Stamp2Str(stamp)
	timer := Str2Time(stampStr)
	return timer
}

func ListDir(dirPth string) (files []string, files1 []string, err error) {
	var fileNames []string
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, nil, err
	}
	PthSep := string(os.PathSeparator)
	for _, fi := range dir {
		if fi.IsDir() {
			files1 = append(files1, dirPth+PthSep+fi.Name())
			ListDir(dirPth + PthSep + fi.Name())
		} else {
			fileNames = append(fileNames, fi.Name())
		}
	}
	sort.Slice(
		fileNames,
		func(i, j int) bool {
			return sortName(fileNames[i]) < sortName(fileNames[j])
		},
	)
	for _, file := range fileNames {
		files = append(files, dirPth+PthSep+file)
	}
	return files, files1, nil
}

func sortName(filename string) string {
	ext := filepath.Ext(filename)
	name := filename[:len(filename)-len(ext)]
	// split numeric suffix
	i := len(name) - 1
	for ; i >= 0; i-- {
		if '0' > name[i] || name[i] > '9' {
			break
		}
	}
	i++
	// string numeric suffix to uint64 bytes
	// empty string is zero, so integers are plus one
	b64 := make([]byte, 64/8)
	s64 := name[i:]
	if len(s64) > 0 {
		u64, err := strconv.ParseUint(s64, 10, 64)
		if err == nil {
			binary.BigEndian.PutUint64(b64, u64+1)
		}
	}
	// prefix + numeric-suffix + ext
	return name[:i] + string(b64) + ext
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
