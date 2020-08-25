package yts3

import "log"

type LogLevel string

const (
	LogErr  LogLevel = "ERR"
	LogWarn LogLevel = "WARN"
	LogInfo LogLevel = "INFO"
)

type Logger interface {
	Print(level LogLevel, v ...interface{})
}

// GlobalLog creates a Logger that uses the global log.Println() function.
//
// All levels are reported by default. If you pass levels to this function,
// it will act as a level whitelist.
func GlobalLog(levels ...LogLevel) Logger {
	return newStdLog(log.Println, levels...)
}

// StdLog creates a Logger that uses the stdlib's log.Logger type.
//
// All levels are reported by default. If you pass levels to this function,
// it will act as a level whitelist.
func StdLog(log *log.Logger, levels ...LogLevel) Logger {
	return newStdLog(log.Println, levels...)
}

// DiscardLog creates a Logger that discards all messages.
func DiscardLog() Logger {
	return &discardLog{}
}

type stdLog struct {
	log    func(v ...interface{})
	levels map[LogLevel]bool
}

func newStdLog(log func(v ...interface{}), levels ...LogLevel) Logger {
	sl := &stdLog{log: log}
	if len(levels) > 0 {
		sl.levels = map[LogLevel]bool{}
		for _, lv := range levels {
			sl.levels[lv] = true
		}
	}
	return sl
}

func (s *stdLog) Print(level LogLevel, v ...interface{}) {
	if s.levels == nil || s.levels[level] {
		v = append(v, nil)
		copy(v[1:], v)
		v[0] = level
		s.log(v...)
	}
}

type discardLog struct{}

func (d discardLog) Print(level LogLevel, v ...interface{}) {}
