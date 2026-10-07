package org.raft.model;

public record HeartbeatResponse(long term, String leaderId) {}
