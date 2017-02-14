package filings

import (
	"io"
	"os"
	"path/filepath"
)

type ObjectStore interface {
	Put(bucket string, objectID string, data io.ReadCloser) error
	Exists(bucket string, objectID string) bool
	Get(bucket string, objectID string) (io.ReadCloser, error)
}

type fileStore struct {
	dataDir string
}

// NewFileStore returns an ObjectStore backed by a plain filesystem
func NewFileStore(dataDir string) ObjectStore {
	return &fileStore{dataDir: dataDir}
}

func (fs fileStore) objectPath(bucket, id string) (path string, err error) {
	if len(id) < 2 {
		err = errObjectIDTooShort
	}
	return filepath.Join(fs.dataDir, bucket, id[0:2], id), nil
}

func (fs fileStore) Put(bucket string, objectID string, data io.ReadCloser) (err error) {
	defer data.Close()
	path, err := fs.objectPath(bucket, objectID)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm|os.ModeDir)
	if err != nil {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = io.Copy(f, data)
	return
}

func (fs fileStore) Exists(bucket string, objectID string) bool {
	path, err := fs.objectPath(bucket, objectID)
	if err != nil {
		return false
	}
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	if !fi.IsDir() && fi.Size() > 0 {
		return true
	}
	return false
}

func (fs fileStore) Get(bucket string, objectID string) (obj io.ReadCloser, err error) {
	path, err := fs.objectPath(bucket, objectID)
	if err != nil {
		return
	}
	return os.Open(path)
}
