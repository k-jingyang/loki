package giouring

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

func mmap(_ uintptr, length uintptr, prot int, flags int, fd int, offset int64) (xaddr uintptr, err error) {
	data, err := doMmap(fd, offset, int(length), prot, flags)
	if err != nil {
		return 0, err
	}
	return uintptr(unsafe.Pointer(&data[0])), nil
}

func doMmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	return unix.Mmap(fd, offset, length, prot, flags)
}

func munmap(addr uintptr, length uintptr) (err error) {
	return doMunmap(unsafe.Slice((*byte)(unsafe.Pointer(addr)), length))
}

func doMunmap(b []byte) (err error) {
	return unix.Munmap(b)
}
