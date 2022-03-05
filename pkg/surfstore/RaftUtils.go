package surfstore

import (
	"bufio"
	"net"

	//	"google.golang.org/grpc"
	"io"
	"log"

	//	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal(SURF_CLIENT, "Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}

	return
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {

	isCrashedMutex := &sync.RWMutex{}
	isLeaderMutex := &sync.RWMutex{}

	server := RaftSurfstore{
		lock:     sync.Mutex{},
		isLeader: false,
		term:     0,
		log:      make([]*UpdateOperation, 0),

		metaStore: NewMetaStore(blockStoreAddr),

		ip:       ips[id],
		ipList:   ips,
		serverId: id,

		commitIndex:    -1,
		lastApplied:    -1,
		pendingCommits: make([]chan bool, 0),

		// newCommitReadyChan: make(chan struct{}, 16),

		isLeaderMutex: isLeaderMutex,
		isLeaderCond:  sync.NewCond(isLeaderMutex),

		isCrashed:      false,
		notCrashedCond: sync.NewCond(isCrashedMutex),
		isCrashedMutex: isCrashedMutex,

		// nextIndex:  make(map[int]int),
		// matchIndex: make(map[int]int),
	}
	// go server.commitChanSender()
	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)

	ln, err := net.Listen("tcp", server.ip)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}
