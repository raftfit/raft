package org.raft.web;

import org.raft.core.RaftNodeService;
import org.raft.model.RequestVote;
import org.raft.model.VoteResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ElectionController {

    @Autowired
    private RaftNodeService nodeStateMachine;

    @PostMapping("/api/requestVote")
    @ResponseStatus(HttpStatus.OK)
    VoteResponse requestVote(@RequestBody RequestVote requestVote) {
        return nodeStateMachine.onRequestVote(requestVote);
    }
}
