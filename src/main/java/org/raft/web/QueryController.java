package org.raft.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.raft.config.RaftNodeConf;
import org.raft.core.RaftNodeService;
import org.raft.model.ClientEntryResponse;
import org.raft.model.HashtablePost;
import org.raft.model.HashtablePostInput;
import org.raft.model.HashtablePostOutput;
import org.raft.model.LogEntry;
import org.raft.model.LogResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@CrossOrigin
public class QueryController {
    private final long appendTimeout;

    public QueryController(RaftNodeConf conf) {
        this.appendTimeout = conf.entries().appendTimeout();
    }

    @Autowired
    private RaftNodeService node;

    @PostMapping("/query")
    public ResponseEntity<ClientEntryResponse> postQuery(@Valid @RequestBody HashtablePostInput request) {
        ClientEntryResponse response = node.appendCommand(
                new HashtablePost(request.key(), request.value()), appendTimeout);
        HttpStatus status = response.accepted() ? HttpStatus.OK : HttpStatus.CONFLICT;
        return ResponseEntity.status(status).body(response);
    }

    // TODO: чтение ключа из машины состояний (хэш-таблицы), строящейся применением закоммиченных записей
    @GetMapping("/query")
    public ResponseEntity<HashtablePostOutput> getQuery(@RequestParam("key") @NotBlank String key) {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
    }

    @GetMapping("/log")
    public List<HashtablePostOutput> log() {
        return node.getLog().stream()
                .map(LogEntry::command)
                .filter(HashtablePost.class::isInstance)
                .map(HashtablePost.class::cast)
                .map(c -> new HashtablePostOutput(c.key(), c.value()))
                .toList();
    }

    @GetMapping("/api/logs")
    public LogResponse logs() {
        return new LogResponse(node.getSelfId(), node.getCommitIndex(), node.getLog());
    }
}
