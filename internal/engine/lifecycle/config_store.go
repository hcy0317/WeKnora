package lifecycle

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gopkg.in/yaml.v3"
)

type RevisionConflictError struct {
	Expected uint64
	Actual   uint64
}

func (e *RevisionConflictError) Error() string {
	return fmt.Sprintf("engine lifecycle config revision conflict: expected %d, actual %d", e.Expected, e.Actual)
}

type ConfigStore struct {
	path string
	mu   sync.Mutex
}

func NewConfigStore(path string) *ConfigStore {
	return &ConfigStore{path: path}
}

func (s *ConfigStore) Load() (*Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadUnlocked()
}

func (s *ConfigStore) loadUnlocked() (*Config, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("open engine lifecycle config: %w", err)
	}
	defer file.Close()
	return DecodeConfig(file)
}

func (s *ConfigStore) Update(expectedRevision uint64, candidate Config) (*Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, err := s.loadUnlocked()
	if err != nil {
		return nil, err
	}
	if current.Revision != expectedRevision {
		return nil, &RevisionConflictError{Expected: expectedRevision, Actual: current.Revision}
	}

	candidate.Revision = current.Revision + 1
	if err := candidate.Validate(); err != nil {
		return nil, err
	}
	encoded, err := encodeConfig(candidate)
	if err != nil {
		return nil, err
	}
	currentBytes, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("read current engine lifecycle config: %w", err)
	}
	if err := writeAtomicFile(s.path+".previous", currentBytes, 0o600); err != nil {
		return nil, fmt.Errorf("preserve previous engine lifecycle config: %w", err)
	}
	if err := writeAtomicFile(s.path, encoded, 0o600); err != nil {
		return nil, fmt.Errorf("replace engine lifecycle config: %w", err)
	}

	return s.loadUnlocked()
}

func encodeConfig(config Config) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	encoder.SetIndent(2)
	if err := encoder.Encode(config); err != nil {
		return nil, fmt.Errorf("encode engine lifecycle config: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("close engine lifecycle config encoder: %w", err)
	}
	return buffer.Bytes(), nil
}

func writeAtomicFile(path string, contents []byte, mode os.FileMode) (err error) {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".engine-lifecycle-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if err != nil {
			_ = os.Remove(temporaryPath)
		}
	}()

	if err = temporary.Chmod(mode); err != nil {
		return err
	}
	if _, err = temporary.Write(contents); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	if err = atomicReplace(temporaryPath, path); err != nil {
		return err
	}
	return nil
}
