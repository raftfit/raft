package org.raft.web;

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

    private static final long APPEND_TIMEOUT_MS = 5000;

    @Autowired
    private RaftNodeService node;

    @PostMapping("/api/entries")
    public ResponseEntity<ClientEntryResponse> entries(@RequestBody ClientEntryRequest request) {
        ClientEntryResponse response = node.appendCommand(request.command(), APPEND_TIMEOUT_MS);
        HttpStatus status = response.accepted() ? HttpStatus.OK : HttpStatus.CONFLICT;
        return ResponseEntity.status(status).body(response);
    }

    @GetMapping("/api/logs")
    public LogResponse logs() {
        return new LogResponse(node.getSelfId(), node.getCommitIndex(), node.getLog());
    }
}
