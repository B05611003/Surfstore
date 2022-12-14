package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

var muGet sync.Mutex
var muPut sync.Mutex

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	muGet.Lock()
	defer muGet.Unlock()
	return &Block{
		BlockData: bs.BlockMap[blockHash.Hash].BlockData,
		BlockSize: bs.BlockMap[blockHash.Hash].BlockSize,
	}, nil
}

// Given a block of content, add it to the hashmap in blockstore
// Hash value is generated by SHA-256 hash function
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	muPut.Lock()
	defer muPut.Unlock()
	hashBytes := sha256.Sum256(block.BlockData)
	hashString := hex.EncodeToString(hashBytes[:])
	bs.BlockMap[hashString] = block
	return &Success{
		Flag: true,
	}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {

	hashOut := make([]string, 0)
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; ok {
			hashOut = append(hashOut, hash)
		}
	}
	return &BlockHashes{
		Hashes: hashOut,
	}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
