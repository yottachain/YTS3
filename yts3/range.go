package yts3

import (
	"strconv"
	"strings"
)

type ObjectRange struct {
	Start, Length int64
}
type ObjectRangeRequest struct {
	Start, End int64
	FromEnd    bool
}

const RangeNoEnd = -1

func (o *ObjectRangeRequest) Range(size int64) (*ObjectRange, error) {
	if o == nil {
		return nil, nil
	}

	var start, length int64

	if !o.FromEnd {
		start = o.Start
		end := o.End

		if o.End == RangeNoEnd {
			// If no end is specified, range extends to end of the file.
			length = size - start
		} else {
			length = end - start + 1
		}

	} else {
		// If no start is specified, end specifies the range start relative
		// to the end of the file.
		end := o.End
		start = size - end
		length = size - start
	}

	if start < 0 || length < 0 || start >= size {
		return nil, ErrInvalidRange
	}

	if start+length > size {
		return &ObjectRange{Start: start, Length: size - start}, nil
	}

	return &ObjectRange{Start: start, Length: length}, nil
}

func parseRangeHeader(s string) (*ObjectRangeRequest, error) {
	if s == "" {
		return nil, nil
	}

	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, ErrInvalidRange
	}

	ranges := strings.Split(s[len(b):], ",")
	if len(ranges) > 1 {
		return nil, ErrorMessage(ErrNotImplemented, "multiple ranges not supported")
	}

	rnge := strings.TrimSpace(ranges[0])
	if len(rnge) == 0 {
		return nil, ErrInvalidRange
	}

	i := strings.Index(rnge, "-")
	if i < 0 {
		return nil, ErrInvalidRange
	}

	var o ObjectRangeRequest

	start, end := strings.TrimSpace(rnge[:i]), strings.TrimSpace(rnge[i+1:])
	if start == "" {
		o.FromEnd = true

		i, err := strconv.ParseInt(end, 10, 64)
		if err != nil {
			return nil, ErrInvalidRange
		}
		o.End = i

	} else {
		i, err := strconv.ParseInt(start, 10, 64)
		if err != nil || i < 0 {
			return nil, ErrInvalidRange
		}
		o.Start = i
		if end != "" {
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil || o.Start > i {
				return nil, ErrInvalidRange
			}
			o.End = i
		} else {
			o.End = RangeNoEnd
		}
	}

	return &o, nil
}
