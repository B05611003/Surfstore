package surfstore

import (
	context "context"
	"fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var muMetaGet sync.Mutex
var muMetaUpdate sync.Mutex

type MetaStore struct {
	muMeta         sync.Mutex
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.muMeta.Lock()
	defer m.muMeta.Unlock()

	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.muMeta.Lock()
	defer m.muMeta.Unlock()

	fileName := fileMetaData.Filename
	// if the file already esist in the MataStore
	if serverFileMetaData, ok := m.FileMetaMap[fileName]; ok {
		// if the version has error
		if fileMetaData.Version-serverFileMetaData.Version != 0 {
			return &Version{
				Version: -1,
			}, fmt.Errorf("file version error: should be %d but %d", serverFileMetaData.Version+1, fileMetaData.Version)
		}
		(*serverFileMetaData).Version += 1
		(*serverFileMetaData).BlockHashList = fileMetaData.BlockHashList

	} else {
		m.FileMetaMap[fileName] = &FileMetaData{
			Filename:      fileMetaData.Filename,
			Version:       fileMetaData.Version,
			BlockHashList: fileMetaData.BlockHashList,
		}
	}

	return &Version{
		Version: m.FileMetaMap[fileName].Version,
	}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
		muMeta:         sync.Mutex{},
	}
}
