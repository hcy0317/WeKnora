//go:build !windows

package hostcontroller

import "sync"

var portableOwners = struct {
	sync.Mutex
	names map[string]struct{}
}{names: make(map[string]struct{})}

type portableOwnership struct {
	name string
	once sync.Once
}

func AcquireOwnership(name string) (Ownership, error) {
	portableOwners.Lock()
	defer portableOwners.Unlock()
	if _, exists := portableOwners.names[name]; exists {
		return nil, ErrOwnerConflict
	}
	portableOwners.names[name] = struct{}{}
	return &portableOwnership{name: name}, nil
}

func (o *portableOwnership) Close() error {
	o.once.Do(func() {
		portableOwners.Lock()
		delete(portableOwners.names, o.name)
		portableOwners.Unlock()
	})
	return nil
}
