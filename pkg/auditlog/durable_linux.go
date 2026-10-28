//go:build linux

package auditlog

import (
	"os"
	"syscall"
)

func datasync(f *os.File) error { return syscall.Fdatasync(int(f.Fd())) }

func preallocate(f *os.File, off, n int64) error {
	return syscall.Fallocate(int(f.Fd()), 0, off, n)
}
