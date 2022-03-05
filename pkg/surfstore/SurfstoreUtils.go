package surfstore

import (
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path"
)

// status type
const (
	Unchanged int = 0
	Changed       = 1
	New           = 2
	Delete        = 3
)

// Implement the logic for a client syncing with the server .
func ClientSync(client RPCClient) {
	fmt.Printf("%s start func ClientSync\n", client.BaseDir)
	/*
		The client should first scan the base directory, and for each file, compute that file’s hash list.
		The client should then consult the local index file and compare the results, to see whether
		(1) there are now new files in the base directory that aren’t in the index file
		(2) files that are in the index file, but have changed since the last time the client was executed
			(i.e., the hash list is different).
	*/

	// read the directory and get file info of the client
	dirInfo, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatalf("error while reading directory : %v", err)
	}

	dirInfoMap := make(map[string]fs.FileInfo)
	for _, fileInfo := range dirInfo {
		dirInfoMap[fileInfo.Name()] = fileInfo
	}

	/*
		Read the index.txt to get the current client matadata info
	*/
	// lookup index.txt file
	indexFileName := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(indexFileName); os.IsNotExist(err) { // create empty index.txt if DNE
		indexFile, err := os.Create(indexFileName)
		if err != nil {
			log.Fatalf("error while creating index.txt, %v", err)
		}
		defer indexFile.Close()
	}

	// load the index.txt file
	indexFileMetaDataMap, err := LoadMetaFromMetaFile(client.BaseDir) // map[fileName] -> *FileMetaData in index file
	if err != nil {
		log.Fatalf("error while reading index.txt, %v", err)
	}

	// compare the local dir to local index.txt
	localFileMetaDataMap, localStatus, err := syncLocal(client, dirInfoMap, indexFileMetaDataMap)
	if err != nil {
		log.Fatalf("error while local sync, %v", err)
	}
	fmt.Printf("localFileMeta:%v\n", localFileMetaDataMap)
	// download the remote index.txt
	remoteFileMetaMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteFileMetaMap)
	if err != nil {
		log.Fatalf("error while getting server info map, %v", err)
	}

	// check the server file base on client
	// upload if needed
	for fileName, localFileMetaData := range localFileMetaDataMap {
		if remoteFileMetaData, ok := remoteFileMetaMap[fileName]; !ok {
			// file metadata not exits on server, it is deleted file or need upload
			err := upload(client, localFileMetaData, &localFileMetaDataMap, localStatus[fileName])
			if err != nil {
				log.Println("Upload file failed: ", err)
			}
		} else {
			// file both exist on server and local
			if remoteFileMetaData.Version > localFileMetaData.Version {
				download(client, remoteFileMetaData, &localFileMetaDataMap)
			} else if remoteFileMetaData.Version == localFileMetaData.Version && localStatus[fileName] != Unchanged { //todo
				if localStatus[fileName] == New {
					download(client, remoteFileMetaData, &localFileMetaDataMap)
				} else {
					upload(client, localFileMetaData, &localFileMetaDataMap, localStatus[fileName])
				}
			} else if remoteFileMetaData.Version < localFileMetaData.Version {
				log.Println("something went wrong : local version highter than remote")
			}
		}
	}

	// check the client file base on server metadata
	// download if needed
	for fileName, remoteFileMetaData := range remoteFileMetaMap {
		if _, ok := localFileMetaDataMap[fileName]; !ok {
			err := download(client, remoteFileMetaData, &localFileMetaDataMap)
			if err != nil {
				log.Printf("error while download server new file: %v", err)
			}
		}
	}

	WriteMetaFile(localFileMetaDataMap, client.BaseDir)
	fmt.Printf("%s done func ClientSyn\n", client.BaseDir)
	fmt.Printf("client: %s,info: %v\n", client.BaseDir, localFileMetaDataMap)

}

func syncLocal(client RPCClient, dirInfoMap map[string]fs.FileInfo, indexFileMetaDataMap map[string]*FileMetaData) (map[string]*FileMetaData, map[string]int, error) {
	fmt.Printf("%s start func syncLocal\n", client.BaseDir)
	localFileMetaDataMap := make(map[string]*FileMetaData)
	localFileStatus := make(map[string]int)

	// iterate through all the file in dirInfoMap and compare to indexfile
	for fileName, fileInfo := range dirInfoMap {
		if fileName == "index.txt" {
			continue
		}
		if indexFileMetaData, ok := indexFileMetaDataMap[fileName]; ok {
			// file found in dir, compare them
			hashes, err := hashFile(path.Join(client.BaseDir, fileName), fileInfo, int64(client.BlockSize))
			if err != nil {
				return nil, nil, fmt.Errorf("error while hashing file : %v", err)
			}

			if compareHashList(hashes, indexFileMetaData.BlockHashList) {
				localFileStatus[fileName] = Changed
				localFileMetaDataMap[fileName] = &FileMetaData{
					Filename:      fileName,
					Version:       indexFileMetaData.Version,
					BlockHashList: hashes,
				}
			} else {
				localFileStatus[fileName] = Unchanged
				localFileMetaDataMap[fileName] = indexFileMetaData
			}
		} else {
			hashes, err := hashFile(path.Join(client.BaseDir, fileName), fileInfo, int64(client.BlockSize))
			if err != nil {
				return nil, nil, fmt.Errorf("error while hashing file : %v", err)
			}
			localFileStatus[fileName] = New
			localFileMetaDataMap[fileName] = &FileMetaData{
				Filename:      fileName,
				Version:       1,
				BlockHashList: hashes,
			}

		}
	}

	// iterate through all the indexfile and compare to dirInfoMap
	for fileName, fileMetaData := range indexFileMetaDataMap {
		if _, ok := dirInfoMap[fileName]; !ok {
			// file has deleted
			if len(fileMetaData.BlockHashList) != 1 || fileMetaData.BlockHashList[0] != "0" { // if the file has not been deleted yet
				localFileStatus[fileName] = Delete
				localFileMetaDataMap[fileName] = &FileMetaData{
					Filename:      fileName,
					Version:       fileMetaData.Version,
					BlockHashList: []string{"0"},
				}
			} else {
				localFileStatus[fileName] = Unchanged
				localFileMetaDataMap[fileName] = fileMetaData
			}
		}
	}
	fmt.Printf("%s finish func syncLocal\n", client.BaseDir)
	return localFileMetaDataMap, localFileStatus, nil
}

// given a file name and output the hashList of the file
func hashFile(fileName string, fileInfo fs.FileInfo, blockSize int64) ([]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("error while onpeing file : %v", err)
	}
	defer file.Close()
	fileSize := fileInfo.Size()
	blockNum := int((fileSize + blockSize - 1) / blockSize)

	hashList := make([]string, blockNum)
	for i := 0; i < blockNum; i++ {
		buf := make([]byte, blockSize)
		size, err := file.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("error while reading file : %v", err)
		}
		buf = buf[:size]
		hashList[i] = GetBlockHashString(buf)
	}
	return hashList, nil
}

// give to hash list and compare if it is different
func compareHashList(hash1 []string, hash2 []string) bool {
	if len(hash1) != len(hash2) {
		return true
	}
	for i := range hash1 {
		if hash1[i] != hash2[i] {
			return true
		}
	}
	return false
}

// if the file DNE in server, and not the deleted file
// upload it to the server
func upload(client RPCClient, localFileMetaData *FileMetaData, localFileMetaDataMap *map[string]*FileMetaData, status int) error {
	fmt.Printf("%s start func upload\n", client.BaseDir)
	fileName := path.Join(client.BaseDir, localFileMetaData.Filename)

	if status == Delete {

	} else if status == Changed || status == New {
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("Open file Error: %v", err)
		}
		defer file.Close()
		fileInfo, err := os.Stat(fileName)
		if err != nil {
			log.Printf("Get Status error: %v", err)
			return err
		}
		fileSize := fileInfo.Size()
		blockNum := int((fileSize + int64(client.BlockSize) - 1) / int64(client.BlockSize))
		for i := 0; i < blockNum; i++ {
			var block Block
			block.BlockData = make([]byte, client.BlockSize)
			size, err := file.Read(block.BlockData)
			if err != nil && err != io.EOF {
				log.Printf("read block error: %v", err)
				return err
			}
			block.BlockData = block.BlockData[:size]
			block.BlockSize = int32(size)

			var succ bool
			var blockStoreAddr string
			if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
				log.Printf("GetBlockStoreAddr error: %v", err)
				// log.Fatal(err)
			}
			err = client.PutBlock(&block, blockStoreAddr, &succ)
			if err != nil {
				log.Printf("Putblock error: %v", err)
				return err
			}
		}
	}

	if err := client.UpdateFile(localFileMetaData, &localFileMetaData.Version); err != nil {
		// update failed, new version of file on server
		newRemoteFileMetaMap := make(map[string]*FileMetaData)
		err = client.GetFileInfoMap(&newRemoteFileMetaMap)
		if err != nil {
			log.Fatalf("error while getting server info map, %v", err)
		}
		fmt.Printf("upload failed: ver%d vs ver %d\n", localFileMetaData.Version, newRemoteFileMetaMap[localFileMetaData.Filename].Version)
		download(client, newRemoteFileMetaMap[localFileMetaData.Filename], localFileMetaDataMap)
		fmt.Printf("%d\n", newRemoteFileMetaMap[localFileMetaData.Filename].Version)
	} else {
		(*localFileMetaDataMap)[localFileMetaData.Filename] = localFileMetaData
		// (*localFileMetaDataMap)[localFileMetaData.Filename].Version += 1
	}
	fmt.Printf("%s done func upload\n", client.BaseDir)
	return nil

}

func download(client RPCClient, remoteFileMetaData *FileMetaData, localFileMetaDataMap *map[string]*FileMetaData) error {
	fmt.Printf("%s start func download\n", client.BaseDir)
	fileName := path.Join(client.BaseDir, remoteFileMetaData.Filename)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		os.Create(fileName)
	} else {
		os.Truncate(fileName, 0)
	}

	if checkDelete(remoteFileMetaData) {
		os.Remove(fileName)
		(*localFileMetaDataMap)[remoteFileMetaData.Filename].Version = remoteFileMetaData.Version
		(*localFileMetaDataMap)[remoteFileMetaData.Filename].BlockHashList = remoteFileMetaData.BlockHashList
		return nil
	}

	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal("error opening files")
	}
	defer file.Close()

	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	for _, hash := range remoteFileMetaData.BlockHashList {
		var block Block
		err := client.GetBlock(hash, blockStoreAddr, &block)
		if err != nil {
			log.Fatalf("Get block error: %v", err)
		}

		content := block.BlockData
		_, err = file.Write(content)
		if err != nil {
			log.Fatalf("Write file error: %v", err)
		}
	}

	(*localFileMetaDataMap)[remoteFileMetaData.Filename] = &FileMetaData{
		Version:       remoteFileMetaData.Version,
		Filename:      remoteFileMetaData.Filename,
		BlockHashList: remoteFileMetaData.BlockHashList,
	}
	fmt.Printf("%s done func download\n", client.BaseDir)
	return nil
}

// check if it is a deleted file from Metadata
func checkDelete(fileMetaData *FileMetaData) bool {
	return len(fileMetaData.BlockHashList) == 1 && fileMetaData.BlockHashList[0] == "0"
}
