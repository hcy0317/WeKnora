package hostcontroller

import "errors"

var ErrOwnerConflict = errors.New("engine Docker owner is already active")

type Ownership interface {
	Close() error
}
