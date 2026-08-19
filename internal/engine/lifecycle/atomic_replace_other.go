//go:build !windows

package lifecycle

import "os"

func atomicReplace(source, destination string) error {
	return os.Rename(source, destination)
}
