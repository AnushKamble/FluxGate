package telemetry

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l LogLevel) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger provides thread-safe structured logging with optional sampling.
type Logger struct {
	mu           sync.Mutex
	out          io.Writer
	level        LogLevel
	sampleRate   uint64 // Log 1 out of every N debug/info events if > 1
	sampleCount  uint64
	defaultAttrs map[string]interface{}
}

func NewLogger(out io.Writer, level LogLevel, sampleRate uint64, defaultAttrs map[string]interface{}) *Logger {
	if out == nil {
		out = os.Stdout
	}
	attrs := make(map[string]interface{})
	for k, v := range defaultAttrs {
		attrs[k] = v
	}
	return &Logger{
		out:          out,
		level:        level,
		sampleRate:   sampleRate,
		defaultAttrs: attrs,
	}
}

func (l *Logger) With(key string, val interface{}) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	newAttrs := make(map[string]interface{})
	for k, v := range l.defaultAttrs {
		newAttrs[k] = v
	}
	newAttrs[key] = val
	return &Logger{
		out:          l.out,
		level:        l.level,
		sampleRate:   l.sampleRate,
		defaultAttrs: newAttrs,
	}
}

func (l *Logger) log(level LogLevel, msg string, keyvals ...interface{}) {
	if level < l.level {
		return
	}

	// Sampling for high-throughput info/debug logs
	if l.sampleRate > 1 && (level == LevelDebug || level == LevelInfo) {
		cnt := atomic.AddUint64(&l.sampleCount, 1)
		if cnt%l.sampleRate != 0 {
			return
		}
	}

	entry := make(map[string]interface{})
	entry["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
	entry["level"] = level.String()
	entry["message"] = msg

	l.mu.Lock()
	for k, v := range l.defaultAttrs {
		entry[k] = v
	}
	l.mu.Unlock()

	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			k := fmt.Sprintf("%v", keyvals[i])
			entry[k] = keyvals[i+1]
		}
	}

	data, err := json.Marshal(entry)
	if err != nil {
		data = []byte(fmt.Sprintf(`{"level":"ERROR","message":"failed to marshal log entry: %s"}`, err.Error()))
	}
	data = append(data, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(data)
}

func (l *Logger) Debug(msg string, keyvals ...interface{}) {
	l.log(LevelDebug, msg, keyvals...)
}

func (l *Logger) Info(msg string, keyvals ...interface{}) {
	l.log(LevelInfo, msg, keyvals...)
}

func (l *Logger) Warn(msg string, keyvals ...interface{}) {
	l.log(LevelWarn, msg, keyvals...)
}

func (l *Logger) Error(msg string, keyvals ...interface{}) {
	l.log(LevelError, msg, keyvals...)
}
