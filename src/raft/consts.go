package raft

const (
	NullCandidate               = -1 //  means this server hasn't voted yet for the current term
	ApplyMsgIntervalMS          = 3
	HeartBeatIntervalMS         = 100
	RespWaitingTimeoutMS        = 125
	ElectionTimeoutLowerBound   = 300
	ElectionTimeoutUpBoundRange = 500
)
