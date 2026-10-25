package org.raft.model;

public record ClientEntryResponse(boolean accepted, long index, long term, String leaderId) {}
