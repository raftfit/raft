package org.raft.api;

public record VoteResponse(long term, String voterId, boolean voteGranted) {}
