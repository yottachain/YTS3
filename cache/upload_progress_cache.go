package cache

import (
	"time"

	"github.com/patrickmn/go-cache"
)

var upload_progress_CACHE = cache.New(time.Duration(100000)*time.Second, time.Duration(100000)*time.Second)
