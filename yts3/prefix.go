package yts3

import (
	"fmt"
	"net/url"
	"strings"
)

type Prefix struct {
	HasPrefix bool
	Prefix    string

	HasDelimiter bool
	Delimiter    string
}

func prefixFromQuery(query url.Values) Prefix {
	prefix := Prefix{
		Prefix:    query.Get("prefix"),
		Delimiter: query.Get("delimiter"),
	}
	_, prefix.HasPrefix = query["prefix"]
	_, prefix.HasDelimiter = query["delimiter"]
	return prefix
}

func NewPrefix(prefix, delim *string) (p Prefix) {
	if prefix != nil {
		p.HasPrefix, p.Prefix = true, *prefix
	}
	if delim != nil {
		p.HasDelimiter, p.Delimiter = true, *delim
	}
	return p
}

func NewFolderPrefix(prefix string) (p Prefix) {
	p.HasPrefix, p.Prefix = true, prefix
	p.HasDelimiter, p.Delimiter = true, "/"
	return p
}

func (p Prefix) FilePrefix() (path, remaining string, ok bool) {
	if !p.HasPrefix || !p.HasDelimiter || p.Delimiter != "/" {
		return "", "", ok
	}

	idx := strings.LastIndexByte(p.Prefix, '/')
	if idx < 0 {
		return "", p.Prefix, true
	} else {
		return p.Prefix[:idx], p.Prefix[idx+1:], true
	}
}

func (p Prefix) Match(key string, match *PrefixMatch) (ok bool) {
	if !p.HasPrefix && !p.HasDelimiter {
		if match != nil {
			*match = PrefixMatch{Key: key, MatchedPart: key}
		}
		return true
	}

	if !p.HasDelimiter {
		if strings.HasPrefix(key, p.Prefix) {
			if match != nil {
				*match = PrefixMatch{Key: key, MatchedPart: p.Prefix}
			}
			return true
		}
		return false
	}

	keyParts := strings.Split(strings.TrimLeft(key, p.Delimiter), p.Delimiter)
	preParts := strings.Split(strings.TrimLeft(p.Prefix, p.Delimiter), p.Delimiter)

	if len(keyParts) < len(preParts) {
		return false
	}
	appendDelim := len(keyParts) != len(preParts)
	matched := 0

	last := len(preParts) - 1
	for i := 0; i < len(preParts); i++ {
		if i == last {
			if !strings.HasPrefix(keyParts[i], preParts[i]) {
				return false
			}

		} else {
			if keyParts[i] != preParts[i] {
				return false
			}
		}
		matched++
	}

	if matched == 0 {
		return false
	}

	out := strings.Join(keyParts[:matched], p.Delimiter)
	if appendDelim {
		out += p.Delimiter
	}

	if match != nil {
		*match = PrefixMatch{Key: key, CommonPrefix: out != key, MatchedPart: out}
	}
	return true
}

func (p Prefix) String() string {
	if p.HasDelimiter {
		return fmt.Sprintf("prefix:%q, delim:%q", p.Prefix, p.Delimiter)
	} else {
		return fmt.Sprintf("prefix:%q", p.Prefix)
	}
}

type PrefixMatch struct {
	Key string

	CommonPrefix bool

	MatchedPart string
}

func (match *PrefixMatch) AsCommonPrefix() CommonPrefix {
	return CommonPrefix{Prefix: match.MatchedPart}
}
