package raft

const (
	NullCandidate               = -1 //  means this server hasn't voted yet for the current term
	ApplyMsgIntervalMS          = 5
	HeartBeatIntervalMS         = 100
	RespWaitingTimeoutMS        = 30
	ElectionTimeoutLowerBound   = 300
	ElectionTimeoutUpBoundRange = 600
)
