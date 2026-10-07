package org.raft.model;

public record RequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) {}

