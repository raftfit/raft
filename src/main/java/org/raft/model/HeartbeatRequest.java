package org.raft.model;

import java.util.ArrayList;

public record HeartbeatRequest(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        long leaderCommit,
        ArrayList<LogEntry> raftLogs
) { }
