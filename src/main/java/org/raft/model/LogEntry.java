package org.raft.model;

public record LogEntry(Command command, long term) {}
