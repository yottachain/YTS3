package yts3

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var RequestNum *int32 = new(int32)

func (g *Yts3) routeBase(w http.ResponseWriter, r *http.Request) {
	//defer env.TracePanic("routeBase")
	defer func() {
		if rec := recover(); rec != nil {
			env.TraceError("routeBase")
			g.httpError(w, r, errors.New("Service ERR."))
		}
	}()

	var (
		path   = strings.Trim(r.URL.Path, "/")
		parts  = strings.SplitN(path, "/", 2)
		bucket = parts[0]
		query  = r.URL.Query()
		object = ""
		err    error
	)
	url := r.URL.Path
	logrus.Infof("Url:%s\n", url)
	hdr := w.Header()

	id := fmt.Sprintf("%016X", g.nextRequestID())
	hdr.Set("x-amz-id-2", base64.StdEncoding.EncodeToString([]byte(id+id+id+id))) // x-amz-id-2 is 48 bytes of random stuff
	hdr.Set("x-amz-request-id", id)
	hdr.Set("Server", "AmazonS3")

	if len(parts) == 2 {
		object = url[len(bucket)+2:]
		//hdr.Set("Content-Length",r.Header.Get("Content-Length"))
		//logrus.Infof("Content-Length:::::::::::::::::%s\n",r.Header.Get("Content-Length"))
		//logrus.Infof("ContentLength:::::::::::::::::%s\n",r.Header.Get("ContentLength"))
		//object = parts[1]
	}

	count := atomic.AddInt32(RequestNum, 1)
	defer atomic.AddInt32(RequestNum, -1)
	logrus.Infof("All Request Number: %d\n", count)
	if count > 100 {
		return
	}

	//hdr.Set("Content-Length", r.Header.Get("Content-Length"))
	if uploadID := UploadID(query.Get("uploadId")); uploadID != "" {
		err = g.routeMultipartUpload(bucket, object, uploadID, w, r)

	} else if _, ok := query["uploads"]; ok {
		err = g.routeMultipartUploadBase(bucket, object, w, r)

	} else if _, ok := query["versioning"]; ok {
		// err = g.routeVersioning(bucket, w, r)

	} else if _, ok := query["versions"]; ok {
		// err = g.routeVersions(bucket, w, r)

	} else if versionID := versionFromQuery(query["versionId"]); versionID != "" {
		// err = g.routeVersion(bucket, object, VersionID(versionID), w, r)

	} else if bucket != "" && object != "" {
		err = g.routeObject(bucket, object, w, r)

	} else if bucket != "" {
		err = g.routeBucket(bucket, w, r)

	} else if r.Method == "GET" {
		err = g.listBuckets(w, r)

	} else {
		http.NotFound(w, r)
		return
	}

	if err != nil {
		g.httpError(w, r, err)
	}
}

func (g *Yts3) routeMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	switch r.Method {
	case "GET":
		return g.listMultipartUploadParts(bucket, object, uploadID, w, r)
	case "PUT":
		return g.putMultipartUploadPart(bucket, object, uploadID, w, r)
	case "DELETE":
		return g.abortMultipartUpload(bucket, object, uploadID, w, r)
	case "POST":
		return g.completeMultipartUpload(bucket, object, uploadID, w, r)
	default:
		return ErrMethodNotAllowed
	}
}

// routeObject oandles URLs that contain both a bucket path segment and an
// object path segment.
func (g *Yts3) routeObject(bucket, object string, w http.ResponseWriter, r *http.Request) (err error) {
	switch r.Method {
	case "GET":
		return g.getObject(bucket, object, "", w, r)
	case "HEAD":
		return g.headObject(bucket, object, "", w, r)
		// return g.getObject(bucket, object, "", w, r)
	case "PUT":
		return g.createObject(bucket, object, w, r)
	case "DELETE":
		return g.deleteObject(bucket, object, w, r)
	default:
		return ErrMethodNotAllowed
	}
}

// routeBucket handles URLs that contain only a bucket path segment, not an
// object path segment.
func (g *Yts3) routeBucket(bucket string, w http.ResponseWriter, r *http.Request) (err error) {
	switch r.Method {
	case "GET":
		if _, ok := r.URL.Query()["location"]; ok {
			return g.getBucketLocation(bucket, w, r)
		} else {
			return g.listBucket(bucket, w, r)
		}
	case "PUT":
		return g.createBucket(bucket, w, r)
	case "DELETE":
		return g.deleteBucket(bucket, w, r)

	case "POST":
		if _, ok := r.URL.Query()["delete"]; ok {
			return g.deleteMulti(bucket, w, r)
		} else {
			return g.createObjectBrowserUpload(bucket, w, r)
		}
	default:
		return ErrMethodNotAllowed
	}
}

func (g *Yts3) routeMultipartUploadBase(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	switch r.Method {
	case "GET":
		return g.listMultipartUploads(bucket, w, r)
	case "POST":
		return g.initiateMultipartUpload(bucket, object, w, r)
	default:
		return ErrMethodNotAllowed
	}
}

func versionFromQuery(qv []string) string {
	// The versionId subresource may be the string 'null'; this has been
	// observed coming in via Boto. The S3 documentation for the "DELETE
	// object" endpoint describes a 'null' version explicitly, but we don't
	// want backend implementers to have to special-case this string, so
	// let's hide it in here:
	if len(qv) > 0 && qv[0] != "" && qv[0] != "null" {
		return qv[0]
	}
	return ""
}
