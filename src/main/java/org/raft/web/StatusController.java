package org.raft.web;

import org.raft.core.RaftNodeService;
import org.raft.model.Status;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
public class StatusController {
    @Autowired
    private RaftNodeService node;

    @GetMapping("/api/status")
    @ResponseStatus(HttpStatus.OK)
    public Status status() {
        var selfId = node.getSelfId();
        var role = node.getRole();
        var currentTerm = node.getCurrentTerm();
        var votedFor = node.getVotedFor();
        var leaderId = node.getLeaderId();

        return new Status(selfId, role, currentTerm, votedFor, leaderId);
    }
}
