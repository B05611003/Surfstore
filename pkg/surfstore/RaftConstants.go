package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")

const SURF_CLIENT string = "[Surfstore RPCClient]:"
const SURF_SERVER string = "[Surfstore Server]:"
