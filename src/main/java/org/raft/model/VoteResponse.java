package org.raft.model;

public record VoteResponse(long term, String voterId, boolean voteGranted) {}
