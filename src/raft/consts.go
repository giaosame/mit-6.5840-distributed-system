package raft

const (
	NullCandidate             = -1 //  means this server hasn't voted yet for the current term
	ApplyMsgIntervalMS        = 5
	HeartBeatIntervalMS       = 60
	ElectionTimeoutLowerBound = 160
	ElectionTimeoutRange      = 201
)
