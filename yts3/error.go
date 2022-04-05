package yts3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"time"
)

const (
	ErrNone ErrorCode = ""

	ErrBadDigest ErrorCode = "BadDigest"

	ErrInvalidAccessKeyID ErrorCode = "InvalidAccessKeyID"

	ErrBucketAlreadyExists ErrorCode = "BucketAlreadyExists"

	ErrBucketNotEmpty ErrorCode = "BucketNotEmpty"

	ErrIllegalVersioningConfiguration ErrorCode = "IllegalVersioningConfigurationException"

	ErrIncompleteBody ErrorCode = "IncompleteBody"

	ErrIncorrectNumberOfFilesInPostRequest ErrorCode = "IncorrectNumberOfFilesInPostRequest"

	ErrInlineDataTooLarge ErrorCode = "InlineDataTooLarge"

	ErrInvalidArgument ErrorCode = "InvalidArgument"

	ErrInvalidBucketName ErrorCode = "InvalidBucketName"

	ErrInvalidDigest ErrorCode = "InvalidDigest"

	ErrInvalidRange         ErrorCode = "InvalidRange"
	ErrInvalidToken         ErrorCode = "InvalidToken"
	ErrKeyTooLong           ErrorCode = "KeyTooLongError"
	ErrMalformedPOSTRequest ErrorCode = "MalformedPOSTRequest"

	ErrInvalidPart ErrorCode = "InvalidPart"

	ErrInvalidPartOrder ErrorCode = "InvalidPartOrder"

	ErrInvalidURI ErrorCode = "InvalidURI"

	ErrMetadataTooLarge ErrorCode = "MetadataTooLarge"
	ErrMethodNotAllowed ErrorCode = "MethodNotAllowed"
	ErrMalformedXML     ErrorCode = "MalformedXML"

	ErrMissingContentLength ErrorCode = "MissingContentLength"

	ErrNoSuchBucket ErrorCode = "NoSuchBucket"

	ErrNoSuchKey ErrorCode = "NoSuchKey"

	ErrNoSuchUpload ErrorCode = "NoSuchUpload"

	ErrNoSuchVersion ErrorCode = "NoSuchVersion"

	ErrRequestTimeTooSkewed ErrorCode = "RequestTimeTooSkewed"
	ErrTooManyBuckets       ErrorCode = "TooManyBuckets"
	ErrNotImplemented       ErrorCode = "NotImplemented"

	ErrInternal      ErrorCode = "InternalError"
	ErrAuthorization ErrorCode = "NotAuthorization"
)

const (
	ErrInternalPageNotImplemented InternalErrorCode = "PaginationNotImplemented"
)

type errorResponse interface {
	Error
	enrich(requestID string)
}

func ensureErrorResponse(err error, requestID string) Error {
	switch err := err.(type) {
	case errorResponse:
		err.enrich(requestID)
		return err

	case ErrorCode:
		return &ErrorResponse{
			Code:      err,
			RequestID: requestID,
			Message:   string(err),
		}

	default:
		return &ErrorResponse{
			Code:      ErrInternal,
			Message:   "Internal Error",
			RequestID: requestID,
		}
	}
}

type Error interface {
	error
	ErrorCode() ErrorCode
}

type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`

	Code      ErrorCode
	Message   string `xml:",omitempty"`
	RequestID string `xml:"RequestId,omitempty"`
	HostID    string `xml:"HostId,omitempty"`
}

func (e *ErrorResponse) ErrorCode() ErrorCode { return e.Code }

func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (r *ErrorResponse) enrich(requestID string) {
	r.RequestID = requestID
}

func ErrorMessage(code ErrorCode, message string) error {
	return &ErrorResponse{Code: code, Message: message}
}

func ErrorMessagef(code ErrorCode, message string, args ...interface{}) error {
	return &ErrorResponse{Code: code, Message: fmt.Sprintf(message, args...)}
}

type ErrorInvalidArgumentResponse struct {
	ErrorResponse

	ArgumentName  string `xml:"ArgumentName"`
	ArgumentValue string `xml:"ArgumentValue"`
}

func ErrorInvalidArgument(name, value, message string) error {
	return &ErrorInvalidArgumentResponse{
		ErrorResponse: ErrorResponse{Code: ErrInvalidArgument, Message: message},
		ArgumentName:  name, ArgumentValue: value}
}

type ErrorCode string

func (e ErrorCode) ErrorCode() ErrorCode { return e }
func (e ErrorCode) Error() string        { return string(e) }

type InternalErrorCode string

func (e InternalErrorCode) ErrorCode() ErrorCode { return ErrInternal }
func (e InternalErrorCode) Error() string        { return string(ErrInternal) }

func (e ErrorCode) Message() string {
	switch e {
	case ErrInvalidBucketName:
		return `Bucket name must match the regex "^[a-zA-Z0-9.\-_]{1,255}$"`
	case ErrNoSuchBucket:
		return "The specified bucket does not exist"
	case ErrRequestTimeTooSkewed:
		return "The difference between the request time and the current time is too large"
	case ErrMalformedXML:
		return "The XML you provided was not well-formed or did not validate against our published schema"
	default:
		return ""
	}
}

func (e ErrorCode) Status() int {
	switch e {
	case ErrBucketAlreadyExists,
		ErrBucketNotEmpty:
		return http.StatusConflict

	case ErrBadDigest,
		ErrIllegalVersioningConfiguration,
		ErrIncompleteBody,
		ErrIncorrectNumberOfFilesInPostRequest,
		ErrInlineDataTooLarge,
		ErrInvalidArgument,
		ErrInvalidBucketName,
		ErrInvalidDigest,
		ErrInvalidPart,
		ErrInvalidPartOrder,
		ErrInvalidToken,
		ErrInvalidURI,
		ErrKeyTooLong,
		ErrMetadataTooLarge,
		ErrMethodNotAllowed,
		ErrMalformedPOSTRequest,
		ErrMalformedXML,
		ErrTooManyBuckets:
		return http.StatusBadRequest

	case ErrRequestTimeTooSkewed:
		return http.StatusForbidden

	case ErrInvalidRange:
		return http.StatusRequestedRangeNotSatisfiable

	case ErrNoSuchBucket,
		ErrNoSuchKey,
		ErrNoSuchUpload,
		ErrNoSuchVersion:
		return http.StatusNotFound

	case ErrNotImplemented:
		return http.StatusNotImplemented

	case ErrMissingContentLength:
		return http.StatusLengthRequired

	case ErrInternal:
		return http.StatusInternalServerError
	}

	return http.StatusInternalServerError
}

func HasErrorCode(err error, code ErrorCode) bool {
	if err == nil && code == "" {
		return true
	}
	s3err, ok := err.(interface{ ErrorCode() ErrorCode })
	if !ok {
		return false
	}
	return s3err.ErrorCode() == code
}

func IsAlreadyExists(err error) bool {
	return HasErrorCode(err, ErrBucketAlreadyExists)
}

type resourceErrorResponse struct {
	ErrorResponse
	Resource string
}

var _ errorResponse = &resourceErrorResponse{}

func ResourceError(code ErrorCode, resource string) error {
	return &resourceErrorResponse{
		ErrorResponse{Code: code, Message: code.Message()},
		resource,
	}
}

func BucketNotFound(bucket string) error { return ResourceError(ErrNoSuchBucket, bucket) }
func KeyNotFound(key string) error       { return ResourceError(ErrNoSuchKey, key) }

type requestTimeTooSkewedResponse struct {
	ErrorResponse
	ServerTime                 time.Time
	MaxAllowedSkewMilliseconds durationAsMilliseconds
}

var _ errorResponse = &requestTimeTooSkewedResponse{}

func requestTimeTooSkewed(at time.Time, max time.Duration) error {
	code := ErrRequestTimeTooSkewed
	return &requestTimeTooSkewedResponse{
		ErrorResponse{Code: code, Message: code.Message()},
		at, durationAsMilliseconds(max),
	}
}

type durationAsMilliseconds time.Duration

func (m durationAsMilliseconds) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	var s = fmt.Sprintf("%d", time.Duration(m)/time.Millisecond)
	return e.EncodeElement(s, start)
}
