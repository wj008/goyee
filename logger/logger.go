package logger

import (
	"fmt"
	"github.com/wj008/goyee/config"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

const (
	Ldate         = log.Ldate         // the date in the local time zone: 2009/01/23
	Ltime         = log.Ltime         // the time in the local time zone: 01:23:23
	Lmicroseconds = log.Lmicroseconds // microsecond resolution: 01:23:23.123123.  assumes Ltime.
	Llongfile     = log.Llongfile     // full file name and line number: /a/b/c/d.go:23
	Lshortfile    = log.Lshortfile    // final file name element and line number: d.go:23. overrides Llongfile
	LUTC          = log.LUTC          // if Ldate or Ltime is set, use UTC rather than the local time zone
	Lmsgprefix    = log.Lmsgprefix    // move the "prefix" from the beginning of the line to before the message
	LstdFlags     = log.LstdFlags     // initial values for the standard logger
)

type LogSet struct {
	Template string
	LogFile  string
	File     *os.File
}

var lm *LogSet = nil

type Logger struct{}

func (n *Logger) Log(v ...any) {
	Log(v...)
}

func (n *Logger) Logf(format string, v ...any) {
	Logf(format, v...)
}

var (
	Debug = config.Bool("debug", true)
)

func Log(v ...any) {
	if Debug {
		Output(2, fmt.Sprintln(v...))
	}
}

// Logf logs formatted using the default logger
func Logf(format string, v ...any) {
	if Debug {
		Output(2, fmt.Sprintf(format, v...))
	}
}

func SetOutput(template string) {
	lm = &LogSet{
		Template: template,
		LogFile:  "",
		File:     nil,
	}
}

// Flags returns the output flags for the standard logger.
// The flag bits are Ldate, Ltime, and so on.
func Flags() int {
	return log.Flags()
}

// SetFlags sets the output flags for the standard logger.
// The flag bits are Ldate, Ltime, and so on.
func SetFlags(flag int) {
	log.SetFlags(flag)
}

// Prefix returns the output prefix for the standard logger.
func Prefix() string {
	return log.Prefix()
}

// SetPrefix sets the output prefix for the standard logger.
func SetPrefix(prefix string) {
	log.SetPrefix(prefix)
}

// Writer returns the output destination for the standard logger.
func Writer() io.Writer {
	return log.Writer()
}

// These functions write to the standard logger.

// Print calls Output to print to the standard logger.
// Arguments are handled in the manner of fmt.Print.
func Print(v ...any) {
	Output(2, fmt.Sprint(v...))
}

// Printf calls Output to print to the standard logger.
// Arguments are handled in the manner of fmt.Printf.
func Printf(format string, v ...any) {
	Output(2, fmt.Sprintf(format, v...))
}

// Println calls Output to print to the standard logger.
// Arguments are handled in the manner of fmt.Println.
func Println(v ...any) {
	Output(2, fmt.Sprintln(v...))
}

// Fatal is equivalent to Print() followed by a call to os.Exit(1).
func Fatal(v ...any) {
	Output(2, fmt.Sprint(v...))
	os.Exit(1)
}

// Fatalf is equivalent to Printf() followed by a call to os.Exit(1).
func Fatalf(format string, v ...any) {
	Output(2, fmt.Sprintf(format, v...))
	os.Exit(1)
}

// Fatalln is equivalent to Println() followed by a call to os.Exit(1).
func Fatalln(v ...any) {
	Output(2, fmt.Sprintln(v...))
	os.Exit(1)
}

// Panic is equivalent to Print() followed by a call to panic().
func Panic(v ...any) {
	s := fmt.Sprint(v...)
	Output(2, s)
	panic(s)
}

// Panicf is equivalent to Printf() followed by a call to panic().
func Panicf(format string, v ...any) {
	s := fmt.Sprintf(format, v...)
	Output(2, s)
	panic(s)
}

// Panicln is equivalent to Println() followed by a call to panic().
func Panicln(v ...any) {
	s := fmt.Sprintln(v...)
	Output(2, s)
	panic(s)
}

// Output writes the output for a logging event. The string s contains
// the text to print after the prefix specified by the flags of the
// Logger. A newline is appended if the last character of s is not
// already a newline. Calldepth is the count of the number of
// frames to skip when computing the file name and line number
// if Llongfile or Lshortfile is set; a value of 1 will print the details
// for the caller of Output.
func Output(calldepth int, s string) error {
	if lm != nil && lm.Template != "" {
		dateTime := time.Now().In(config.CstZone())
		nowDate := dateTime.Format("2006_01_02")
		logFile := strings.ReplaceAll(lm.Template, "{date}", nowDate)
		if lm.LogFile != logFile {
			if lm.File != nil {
				lm.File.Close()
				lm.File = nil
			}
			lm.LogFile = logFile
			stdout, err := os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
			if err == nil {
				lm.File = stdout
				log.SetOutput(lm.File)
			}
		}
	}
	return log.Output(calldepth+1, s) // +1 for this frame.
}
