package org.raft.model;

import org.raft.core.Role;

public record Status(
        String nodeId,
        Role role,
        long term,
        String votedFor,
        String leaderId
) {}
