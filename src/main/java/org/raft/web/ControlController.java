package org.raft.web;

import org.raft.core.RaftNodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
public class ControlController {

    @Autowired
    private RaftNodeService node;

    @PostMapping("/api/pause")
    public void pause() {
        node.pause();
    }

    @PostMapping("/api/resume")
    public void resume() {
        node.resume();
    }

    @PostMapping("/api/kill")
    public void kill() {
        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
            Runtime.getRuntime().halt(1);
        }).start();
    }
}
