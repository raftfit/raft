package org.raft.web;

import org.raft.core.RaftNodeService;
import org.raft.model.HeartbeatRequest;
import org.raft.model.HeartbeatResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HeartbeatController {
    @Autowired
    private RaftNodeService node;

    @PostMapping("/api/heartbeat")
    @ResponseStatus(HttpStatus.OK)
    public HeartbeatResponse heartbeat(@RequestBody HeartbeatRequest request) {
        return node.onHeartbeat(request);
    }
}