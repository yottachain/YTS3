package yts3

import (
	"encoding/xml"
	"fmt"
	"sort"
	"time"
)

type UserInfo struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type CompletedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}
type CompleteMultipartUploadResult struct {
	Location string `xml:"Location"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	ETag     string `xml:"ETag"`
}

type Content struct {
	Key          string       `xml:"Key"`
	LastModified ContentTime  `xml:"LastModified"`
	ETag         string       `xml:"ETag"`
	Size         int64        `xml:"Size"`
	StorageClass StorageClass `xml:"StorageClass,omitempty"`
	Owner        *UserInfo    `xml:"Owner,omitempty"`
}

type ContentTime struct {
	time.Time
}

type DeleteRequest struct {
	Objects []ObjectID `xml:"Object"`

	Quiet bool `xml:"Quiet"`
}

type MultiDeleteResult struct {
	XMLName xml.Name      `xml:"DeleteResult"`
	Deleted []ObjectID    `xml:"Deleted"`
	Error   []ErrorResult `xml:",omitempty"`
}

type ErrorResult struct {
	XMLName   xml.Name  `xml:"Error"`
	Key       string    `xml:"Key,omitempty"`
	Code      ErrorCode `xml:"Code,omitempty"`
	Message   string    `xml:"Message,omitempty"`
	Resource  string    `xml:"Resource,omitempty"`
	RequestID string    `xml:"RequestId,omitempty"`
}

func (er ErrorResult) String() string {
	return fmt.Sprintf("%s: [%s] %s", er.Key, er.Code, er.Message)
}

type InitiateMultipartUpload struct {
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID UploadID `xml:"UploadId"`
}

type ListBucketResultBase struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	Xmlns   string   `xml:"xmlns,attr"`

	Name string `xml:"Name"`

	IsTruncated bool `xml:"IsTruncated"`

	Delimiter string `xml:"Delimiter,omitempty"`

	Prefix string `xml:"Prefix"`

	MaxKeys int64 `xml:"MaxKeys,omitempty"`

	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	Contents       []*Content     `xml:"Contents"`
}

type GetBucketLocation struct {
	XMLName            xml.Name `xml:"LocationConstraint"`
	Xmlns              string   `xml:"xmlns,attr"`
	LocationConstraint string   `xml:",chardata"`
}

type ListBucketResult struct {
	ListBucketResultBase

	Marker string `xml:"Marker"`

	NextMarker string `xml:"NextMarker,omitempty"`
}

type ListBucketResultV2 struct {
	ListBucketResultBase

	ContinuationToken string `xml:"ContinuationToken,omitempty"`

	KeyCount int64 `xml:"KeyCount,omitempty"`

	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`

	StartAfter string `xml:"StartAfter,omitempty"`
}

type VersionItem interface {
	GetVersionID() VersionID
	setVersionID(v VersionID)
}

type CompleteMultipartUploadRequest struct {
	Parts []CompletedPart `xml:"Part"`
}

func (c CompleteMultipartUploadRequest) partsAreSorted() bool {
	return sort.IntsAreSorted(c.partIDs())
}

func (c CompleteMultipartUploadRequest) partIDs() []int {
	inParts := make([]int, 0, len(c.Parts))
	for _, inputPart := range c.Parts {
		inParts = append(inParts, inputPart.PartNumber)
	}
	sort.Ints(inParts)
	return inParts
}

type DeleteMarker struct {
	XMLName      xml.Name    `xml:"DeleteMarker"`
	Key          string      `xml:"Key"`
	VersionID    VersionID   `xml:"VersionId"`
	IsLatest     bool        `xml:"IsLatest"`
	LastModified ContentTime `xml:"LastModified,omitempty"`
	Owner        *UserInfo   `xml:"Owner,omitempty"`
}

type Version struct {
	XMLName      xml.Name    `xml:"Version"`
	Key          string      `xml:"Key"`
	VersionID    VersionID   `xml:"VersionId"`
	IsLatest     bool        `xml:"IsLatest"`
	LastModified ContentTime `xml:"LastModified,omitempty"`
	Size         int64       `xml:"Size"`

	StorageClass StorageClass `xml:"StorageClass"`

	ETag  string    `xml:"ETag"`
	Owner *UserInfo `xml:"Owner,omitempty"`
}

func NewContentTime(t time.Time) ContentTime {
	return ContentTime{t}
}

type ListBucketVersionsResult struct {
	XMLName        xml.Name       `xml:"ListBucketVersionsResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	Prefix         string         `xml:"Prefix,omitempty"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated"`
	MaxKeys        int64          `xml:"MaxKeys"`

	KeyMarker string `xml:"KeyMarker,omitempty"`

	NextKeyMarker string `xml:"NextKeyMarker,omitempty"`

	VersionIDMarker VersionID `xml:"VersionIdMarker,omitempty"`

	NextVersionIDMarker VersionID `xml:"NextVersionIdMarker,omitempty"`

	Versions []VersionItem

	prefixes map[string]bool
}

type ListMultipartUploadsResult struct {
	Bucket string `xml:"Bucket"`

	KeyMarker string `xml:"KeyMarker,omitempty"`

	UploadIDMarker UploadID `xml:"UploadIdMarker,omitempty"`

	NextKeyMarker      string   `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker UploadID `xml:"NextUploadIdMarker,omitempty"`

	MaxUploads int64 `xml:"MaxUploads,omitempty"`

	Delimiter string `xml:"Delimiter,omitempty"`

	Prefix string `xml:"Prefix,omitempty"`

	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated,omitempty"`

	Uploads []ListMultipartUploadItem `xml:"Upload"`
}

type ListMultipartUploadItem struct {
	Key          string       `xml:"Key"`
	UploadID     UploadID     `xml:"UploadId"`
	Initiator    *UserInfo    `xml:"Initiator,omitempty"`
	Owner        *UserInfo    `xml:"Owner,omitempty"`
	StorageClass StorageClass `xml:"StorageClass,omitempty"`
	Initiated    ContentTime  `xml:"Initiated,omitempty"`
}

type ListMultipartUploadPartsResult struct {
	XMLName xml.Name `xml:"ListPartsResult"`

	Bucket               string       `xml:"Bucket"`
	Key                  string       `xml:"Key"`
	UploadID             UploadID     `xml:"UploadId"`
	StorageClass         StorageClass `xml:"StorageClass,omitempty"`
	Initiator            *UserInfo    `xml:"Initiator,omitempty"`
	Owner                *UserInfo    `xml:"Owner,omitempty"`
	PartNumberMarker     int          `xml:"PartNumberMarker"`
	NextPartNumberMarker int          `xml:"NextPartNumberMarker"`
	MaxParts             int64        `xml:"MaxParts"`
	IsTruncated          bool         `xml:"IsTruncated,omitempty"`

	Parts []ListMultipartUploadPartItem `xml:"Part"`
}

type ListMultipartUploadPartItem struct {
	PartNumber   int         `xml:"PartNumber"`
	LastModified ContentTime `xml:"LastModified,omitempty"`
	ETag         string      `xml:"ETag,omitempty"`
	Size         int64       `xml:"Size"`
}

type CopyObjectResult struct {
	XMLName      xml.Name    `xml:"CopyObjectResult"`
	ETag         string      `xml:"ETag,omitempty"`
	LastModified ContentTime `xml:"LastModified,omitempty"`
}

type MFADeleteStatus string

const (
	MFADeleteNone     MFADeleteStatus = ""
	MFADeleteEnabled  MFADeleteStatus = "Enabled"
	MFADeleteDisabled MFADeleteStatus = "Disabled"
)

type ObjectID struct {
	Key string `xml:"Key"`

	VersionID string `xml:"VersionId,omitempty" json:"VersionId,omitempty"`
}

type StorageClass string

const (
	StorageStandard StorageClass = "STANDARD"
)

type UploadID string

type VersionID string

type VersioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`

	Status VersioningStatus `xml:"Status"`

	MFADelete MFADeleteStatus `xml:"MfaDelete"`
}

type VersioningStatus string

const (
	VersioningNone      VersioningStatus = ""
	VersioningEnabled   VersioningStatus = "Enabled"
	VersioningSuspended VersioningStatus = "Suspended"
)

func (v Version) GetVersionID() VersionID   { return v.VersionID }
func (v *Version) setVersionID(i VersionID) { v.VersionID = i }

type BucketInfo struct {
	Name string `xml:"Name"`

	// CreationDate is required; without it, boto returns the error "('String
	// does not contain a date:', '')"
	CreationDate ContentTime `xml:"CreationDate"`
}

type Storage struct {
	XMLName xml.Name  `xml:"ListAllMyBucketsResult"`
	Xmlns   string    `xml:"xmlns,attr"`
	Owner   *UserInfo `xml:"Owner,omitempty"`
	Buckets Buckets   `xml:"Buckets>Bucket"`
}

type Buckets []BucketInfo
