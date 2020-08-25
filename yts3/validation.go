package yts3

import (
	"net"
	"regexp"
	"strings"
)

var bucketNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9\.-]+)[a-z0-9]$`)

func ValidateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return ErrorMessage(ErrInvalidBucketName, "bucket name must be >= 3 characters and <= 63")
	}
	if !bucketNamePattern.MatchString(name) {
		return ErrorMessage(ErrInvalidBucketName, "bucket must start and end with 'a-z, 0-9', and contain only 'a-z, 0-9, -' in between")
	}

	if net.ParseIP(name) != nil {
		return ErrorMessage(ErrInvalidBucketName, "bucket names must not be formatted as an IP address")
	}

	labels := strings.Split(name, ".")
	for _, label := range labels {
		if !bucketNamePattern.MatchString(label) {
			return ErrorMessage(ErrInvalidBucketName, "label must start and end with 'a-z, 0-9', and contain only 'a-z, 0-9, -' in between")
		}
	}

	return nil
}

var etagPattern = regexp.MustCompile(`^"[a-z0-9]+"$`)

func validETag(v string) bool {
	return etagPattern.MatchString(v)
}
