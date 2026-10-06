package org.raft.core;

import java.util.List;

public record Status(
        String nodeId,
        Role role,
        long term,
        String votedFor,
        String leaderId,
        List<String> peers
) {}
