package org.raft.web;

import org.raft.config.RaftNodeConf;
import org.raft.core.RaftNodeService;
import org.raft.model.ClientEntryRequest;
import org.raft.model.ClientEntryResponse;
import org.raft.model.LogResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
public class EntriesController {
    private final long timeout;

    public EntriesController(RaftNodeConf conf) {
        timeout = conf.entries().appendTimeout();
    }

    @Autowired
    private RaftNodeService node;

    @PostMapping("/api/entries")
    public ResponseEntity<ClientEntryResponse> entries(@RequestBody ClientEntryRequest request) {
        ClientEntryResponse response = node.appendCommand(request.command(), timeout);
        HttpStatus status = response.accepted() ? HttpStatus.OK : HttpStatus.CONFLICT;
        return ResponseEntity.status(status).body(response);
    }

    @GetMapping("/api/logs")
    public LogResponse logs() {
        return new LogResponse(node.getSelfId(), node.getCommitIndex(), node.getLog());
    }
}
