// Response.go
// Description: Response struct for the HTDB library
// for "better and easier" error handling and debugging
// Author: harto.dev

package library

import (
	"encoding/json"
	"strconv"
	"time"
)

type Response struct {
	TimeStamp  string
	StatusCode int
	Message    string
}

const (
	StatusBadRequest          = 400
	StatusSchenaDoesntExist   = 401
	StatusTableDoesntExist    = 402
	StatusFieldDoesntExist    = 403
	StatusSchenaAlreadyExists = 411
	StatusTableAlreadyExists  = 412
	StatusFieldAlreadyExists  = 413
	StatusInvalidName         = 491
	StatusDbError             = 500
	StatusInternalError       = 501
	StatusUnknown             = 600
)

/*
300 Warning
400 Error
500 Database Error
600 Unknown
*/
func NewResponse(statusCode int, message string) Response {
	return Response{TimeStamp: time.Now().Format("2006-01-02 15:04:05"), StatusCode: statusCode, Message: message}
}

func (r Response) Error() string {
	return r.String()
}

func (r Response) IsWarn() bool {
	return r.StatusCode >= 300 && r.StatusCode < 400
}

func (r Response) IsError() bool {
	return r.StatusCode >= 400 && r.StatusCode < 500
}

func (r Response) IsDbError() bool {
	return r.StatusCode >= 500 && r.StatusCode < 600
}

func (r Response) IsUnknown() bool {
	return r.StatusCode >= 600
}

func (r Response) String() string {
	var status string

	if r.IsWarn() {
		status = "\033[33m" + strconv.Itoa(r.StatusCode) + "\033[0m" + " Warning"
	} else if r.IsError() {
		status = "\033[31m" + strconv.Itoa(r.StatusCode) + "\033[0m" + " Error"
	} else if r.IsDbError() {
		status = "\033[36m" + strconv.Itoa(r.StatusCode) + "\033[0m" + " Database Error"
	} else if r.IsUnknown() {
		status = "\033[37m" + strconv.Itoa(r.StatusCode) + "\033[0m" + " Unknown Error"
	}

	return status + " [" + r.TimeStamp + "] " + r.Message
}

func (r Response) JSON() string {
	data, _ := json.MarshalIndent(r, "", "  ")
	return string(data)
}
