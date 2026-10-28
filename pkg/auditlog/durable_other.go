//go:build !linux

package auditlog

import (
	"errors"
	"os"
)

func datasync(f *os.File) error { return f.Sync() }

func preallocate(f *os.File, off, n int64) error {
	return errors.New("auditlog: no preallocation here")
}
