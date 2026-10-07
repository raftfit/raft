package org.raft.model;

public record HeartbeatRequest(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm
) { }
