package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var muServerGet sync.Mutex

type RaftSurfstore struct {
	lock     sync.Mutex
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	ip       string
	ipList   []string
	serverId int64

	commitIndex    int64
	lastApplied    int64
	pendingCommits []chan bool

	isLeaderMutex *sync.RWMutex
	isLeaderCond  *sync.Cond

	rpcConns []*grpc.ClientConn

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

/*
	If the node is the leader, and if a majority of the nodes are working, should return the correct answer;
	If a majority of the nodes are crashed, should block until a majority recover.
	If not the leader, should indicate an error back to the client
*/
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {

	if !s.isLeader || s.isCrashed {
		return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, ERR_NOT_LEADER
	}
	s.countAlive()
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
}

func (s *RaftSurfstore) countAlive() {
	count := 0
	conns := make([]RaftSurfstoreClient, len(s.ipList))
	for idx, addr := range s.ipList {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("error while dial\n")
			return
		}
		client := NewRaftSurfstoreClient(conn)
		conns[idx] = client
	}
	for count < (len(s.ipList)+1)/2 {
		count = 0
		for idx := range s.ipList {

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if state, err := conns[idx].IsCrashed(ctx, &emptypb.Empty{}); !state.IsCrashed && err == nil {
				count++
			}
		}
	}
	return
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	muServerGet.Lock()
	defer muServerGet.Unlock()
	// fmt.Printf("serverID:%d, isLeader:%v \n", s.serverId, s.isLeader)
	if !s.isLeader || s.isCrashed {
		return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, ERR_NOT_LEADER
	}
	s.countAlive()
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
}

// equal the submit command
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return &Version{
			Version: -1,
		}, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()
	if !s.isLeader {
		return &Version{
			Version: -1,
		}, ERR_NOT_LEADER
	}

	// s.lock.Lock()
	// defer s.lock.Unlock()
	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	fmt.Printf("[Server %d]: get update file command with filemetadata:%v\n", s.serverId, filemeta)

	// if len(s.log) > int(s.lastApplied) {
	// 	s.log = s.log[:s.lastApplied+1]
	// }
	s.log = append(s.log, &op)
	if len(s.log) == 3 {
		s.log = s.log[len(s.log)-1:]
	}
	commited := make(chan bool)
	index := len(s.log) - 1
	s.pendingCommits = append(s.pendingCommits, commited)

	go s.AttemptCommit(index)
	success := <-commited
	fmt.Printf("[Server %d] finish commit\n", s.serverId)
	if success && s.isLeader {
		// commited, so send the heartbeat
		s.lastApplied++
		s.SendHeartbeat(ctx, &emptypb.Empty{})
		ver, err := s.metaStore.UpdateFile(ctx, filemeta)
		if err != nil {
			fmt.Printf("[Server %d]: update metadata error!! %v\n", s.serverId, err)
		} else {
			fmt.Printf("[Server %d]: current metaData:%v\n", s.serverId, s.metaStore.FileMetaMap)
		}
		return ver, err
	}
	return &Version{
		Version: -1,
	}, fmt.Errorf("something went worng")
}

// Aux function
// Commit the entry to other follers and count if majority of them reply success
// break if commit success
func (s *RaftSurfstore) AttemptCommit(index int) bool {
	// the index to commit

	// targetId := s.commitIndex + 1
	targetId := int64(index)
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.CommitEntry(int64(i), targetId, commitChan)
		}
	}

	commitCount := 1
	// TODO end try
	for {
		// TODO handle crashed
		commit := <-commitChan
		if commit != nil && commit.Success {
			fmt.Printf("[Server %d] commit success\n", s.serverId)
			commitCount++
		}
		if commitCount > len(s.ipList)/2 {
			s.pendingCommits[len(s.pendingCommits)-1] <- true
			fmt.Printf("[Server %d] major alive\n", s.serverId)
			s.commitIndex = targetId
			break
		}
	}
	return true
}

// Aux function
// connect to the server and try to get the reply if they append the entry
func (s *RaftSurfstore) CommitEntry(serverId, entryId int64, commitChan chan *AppendEntryOutput) {

	addr := s.ipList[serverId]
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return
	}
	client := NewRaftSurfstoreClient(conn)

	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      s.log[entryId : entryId+1],
		LeaderCommit: s.commitIndex,
	}
	if entryId > 0 {
		input.PrevLogIndex = entryId - 1
		input.PrevLogTerm = s.log[entryId-1].Term
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	output, err := client.AppendEntries(ctx, input)
	if err != nil {
		fmt.Printf("[Server %d] commitEntry append error:%v\n", s.serverId, err)
	}
	fmt.Printf("[Server %d] commitEntry append :%v\n", s.serverId, output)
	commitChan <- output
	// TODO update state s.nextIndex

	// TODO handle crashed server

}

//1. Reply false if term < currentTerm (??5.1)
//2. Reply false if log doesn???t contain an entry at prevLogIndex whose term
//matches prevLogTerm (??5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (??5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	isRestore := false
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(input.Entries) == 0 {
		fmt.Printf("[Server %d] get heartbeat\n", s.serverId)
	} else {
		fmt.Printf("[Server %d] get AppendEntries commmand.Input:%v \n", s.serverId, input)
	}
	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}
	if s.term > input.Term {
		return output, fmt.Errorf("old term\n")
	}
	if s.isCrashed {
		if len(input.Entries) == 0 {
			fmt.Printf("[Server %d] But server %d is crashed\n", s.serverId, s.serverId)
			return output, ERR_SERVER_CRASHED
		}
		// try block until restore
		for s.isCrashed {
			s.isCrashedMutex.Lock()
			s.notCrashedCond.Wait()
			fmt.Printf("[Server %d] restored from crashed\n", s.serverId)
			isRestore = true
			s.isCrashedMutex.Unlock()
		}
		// output.Success = true
		// return output, nil
	}

	if input.Term > s.term {
		s.term = input.Term
		if s.isLeader {
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
		}
	}

	//4. Append any new entries not already in the log
	if !s.isLeader && len(input.Entries) != 0 {
		fmt.Printf("[Server %d] log append, before:%v\n", s.serverId, s.log)
		if isRestore {
			if int(s.lastApplied) < len(s.log)-1 {
				s.log = s.log[:s.lastApplied+1]
			}
		}
		s.log = append(s.log, input.Entries...)
		fmt.Printf("[Server %d] log append, now:%v\n", s.serverId, s.log)
	}
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)

	s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	//fmt.Printf("input.LeaderCommit:%d lastApplied:%d, commitIndex:%d\n", input.LeaderCommit, s.lastApplied, s.commitIndex)
	if !isRestore {

		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			fmt.Printf("[Server %d] new commit index syncing metaStore: %v\n", s.serverId, s.metaStore.FileMetaMap)
		}
	}
	// if len(s.log) == 3 {
	// 	s.log = s.log[len(s.log)-1:]
	// }
	output.Success = true

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the logs.
// If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	fmt.Printf("[server %d] is now leader\n", s.serverId)
	s.term++
	s.isLeader = true
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.isLeader {
		return &Success{Flag: false}, nil
	}
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		fmt.Printf("[Server %d] sent heartbeat to server %d\n", s.serverId, idx)
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Println(err)
			return &Success{Flag: false}, nil
		}
		client := NewRaftSurfstoreClient(conn)
		// TODO
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		client.AppendEntries(ctx, input)
		// if err != nil || output.Success == false {
		// 	if err == ERR_SERVER_CRASHED {

		// 	}
		// 	//fmt.Println("some thing went wrong!!")
		// 	return &Success{Flag: false}, nil
		// }
		// if output != nil {
		// 	return &Success{
		// 		Flag: true,
		// 	}, nil
		// }
		// TODO update state s.nextIndex

		// TODO handle crashed server
	}

	return &Success{Flag: true}, nil
}

// DO NOT EDIT
func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isLeader = false
	fmt.Printf("[Server %d] is now crashed\n", s.serverId)
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

// DO NOT EDIT
func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	fmt.Printf("[Server %d] is now recover\n", s.serverId)
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

// DO NOT EDIT
func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}
func (s *RaftSurfstore) CrashedHold(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	s.isCrashedMutex.Lock()
	s.notCrashedCond.Wait()
	s.isCrashedMutex.Unlock()
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

// DO NOT EDIT
func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
