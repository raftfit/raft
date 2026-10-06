package org.raft.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.raft.core.RaftNodeService;
import org.raft.core.Status;

@RestController
public class StatusController {

    private final RaftNodeService node;

    public StatusController(RaftNodeService node) {
        this.node = node;
    }

    @GetMapping("/status")
    public Status status() {
        return node.getStatus();
    }
}
