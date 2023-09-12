package raft

const (
	NullCandidate             = -1 //  means this server hasn't voted yet for the current term
	HeartBeatIntervalMS       = 100
	RespWaitingTimeoutMS      = 125
	ElectionTimeoutLowerBound = 150
	ElectionTimeoutRange      = 201
)
