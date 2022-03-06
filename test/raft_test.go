package SurfTest

import (
	context "context"
	"cse224/proj5/pkg/surfstore"
	"fmt"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state.Term != int64(1) {
			t.Logf("Server %d should be in term %d but %d", idx, 1, state.Term)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state.Term != int64(2) {
			t.Logf("Server should be in term %d", 2)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	goldenMeta := surfstore.NewMetaStore("")
	goldenLog := make([]*surfstore.UpdateOperation, 0)

	test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	filemeta1.Version++
	test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	for i, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			t.Logf("num %d Logs do not match:\n%v\n%v\n", i, goldenLog, state.Log)
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Logf("num %d MetaStore state is not correct:\n%v\n%v\n", i, goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap)
			t.Fail()
		}

	}
}

func TestRaftRecoverable(t *testing.T) {
	t.Logf("leader1 gets a request while all other nodes are crashed. the crashed nodes recover.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	goldenMeta := surfstore.NewMetaStore("")
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	// worker1 := InitDirectoryWorker("test0", SRC_PATH)

	// defer worker1.CleanUp()

	//clients add different files
	// file1 := "multi_file1.txt"

	// err := worker1.AddFile(file1)
	// if err != nil {
	// 	t.FailNow()
	// }
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	//client1 syncs
	eChan := make(chan bool, 1)
	go func() {
		test.Clients[0].UpdateFile(context.Background(), filemeta1)
		fmt.Println("finish")
		eChan <- true
	}()
	time.Sleep(time.Second)
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	if <-eChan {
		fmt.Printf("update done\n")
	}

	for i, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			t.Logf("num %d Logs do not match:\n%v\n%v\n", i, goldenLog, state.Log)
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Logf("num %d MetaStore state is not correct:\n%v\n%v\n", i, goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap)
			t.Fail()
		}

	}

}

func TestRaftLogsCorrectlyOverwritten(t *testing.T) {
	t.Logf("leader1 gets several requests while all other nodes are crashed. leader1 crashes. all other nodes are restored. leader2 gets a request. leader1 is restored.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta3 := &surfstore.FileMetaData{
		Filename:      "testFile3",
		Version:       1,
		BlockHashList: nil,
	}
	goldenMeta := surfstore.NewMetaStore("")
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	//client1 syncs
	eChan := make(chan bool, 1)
	go func() {
		fmt.Println("start1")
		test.Clients[0].UpdateFile(context.Background(), filemeta1)
		fmt.Println("finish1")
		eChan <- true
	}()
	eChan1 := make(chan bool, 1)
	go func() {
		fmt.Println("start2")
		test.Clients[0].UpdateFile(context.Background(), filemeta2)
		fmt.Println("finish2")
		eChan1 <- true
	}()
	time.Sleep(time.Second)
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	eChan2 := make(chan bool, 1)
	go func() {
		fmt.Println("start3")
		test.Clients[1].UpdateFile(context.Background(), filemeta3)
		fmt.Println("finish3")
		eChan2 <- true
	}()
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	if <-eChan {
		fmt.Printf("update done\n")
	}
	if <-eChan1 {
		fmt.Printf("update done\n")
	}
	if <-eChan2 {
		fmt.Printf("update done\n")
	}

	for i, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			t.Logf("num %d Logs do not match:\n%v\n%v\n", i, goldenLog, state.Log)
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Logf("num %d MetaStore state is not correct:\n%v\n%v\n", i, goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap)
			t.Fail()
		}

	}

}
