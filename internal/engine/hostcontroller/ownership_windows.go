//go:build windows

package hostcontroller

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sys/windows"
)

type windowsOwnership struct {
	handle windows.Handle
	once   sync.Once
	err    error
}

func AcquireOwnership(name string) (Ownership, error) {
	namePointer, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateMutex(nil, true, namePointer)
	if errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
		if handle != 0 {
			_ = windows.CloseHandle(handle)
		}
		return nil, ErrOwnerConflict
	}
	if err != nil {
		return nil, fmt.Errorf("create engine owner mutex: %w", err)
	}
	return &windowsOwnership{handle: handle}, nil
}

func (o *windowsOwnership) Close() error {
	o.once.Do(func() {
		if err := windows.ReleaseMutex(o.handle); err != nil {
			o.err = err
		}
		if err := windows.CloseHandle(o.handle); err != nil && o.err == nil {
			o.err = err
		}
	})
	return o.err
}
