package org.raft.model;

import java.util.List;

public record LogResponse(String nodeId, long commitIndex, List<LogEntry> entries) {}
