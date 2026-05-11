package catalog

import "errors"

var (
	ErrNotFound                  = errors.New("catalog: not found")
	ErrAlreadyExists             = errors.New("catalog: already exists")
	ErrConflict                  = errors.New("catalog: conflict")
	ErrStaleEpoch                = errors.New("catalog: stale epoch")
	ErrUnavailable               = errors.New("catalog: unavailable")
	ErrInvalidArgument           = errors.New("catalog: invalid argument")
	ErrUnsupportedImplementation = errors.New("catalog: unsupported implementation")
)
