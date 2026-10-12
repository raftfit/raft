package org.raft.web;

import org.raft.core.RaftNodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

public class EntriesController {
    @Autowired
    private RaftNodeService node;

    @PostMapping("/api/entries")
    public void entries() {

    }

    @GetMapping("/api/logs")
    public void logs() {

    }
}
