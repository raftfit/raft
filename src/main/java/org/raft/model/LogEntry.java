package org.raft.model;

public record LogEntry(String command, long term) {}