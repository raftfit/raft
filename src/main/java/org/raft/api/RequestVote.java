package org.raft.api;

public record RequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) {}

