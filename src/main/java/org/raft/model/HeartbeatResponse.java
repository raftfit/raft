package org.raft.model;

public record HeartbeatResponse(long term, String leaderId, boolean success, long matchIndex) {}
