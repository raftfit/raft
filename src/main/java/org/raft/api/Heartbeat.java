package org.raft.api;

public record Heartbeat(long term, String leaderId) {}
