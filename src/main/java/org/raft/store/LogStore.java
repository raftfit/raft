package org.raft.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.raft.config.RaftNodeConf;
import org.raft.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

@Component
public class LogStore {

    private static final Logger log = LoggerFactory.getLogger(LogStore.class);
    private static final String FILE_SUFFIX = "-raft-log.json";
    private static final String TMP_SUFFIX = ".tmp";

    @Autowired
    private ObjectMapper objectMapper;
    private final Path logFile;

    public LogStore(RaftNodeConf conf) {
        this.logFile = Paths.get(conf.store().dir()).resolve(conf.selfId() + FILE_SUFFIX);
    }

    public Path getLogFile() {
        return logFile;
    }

    public synchronized void save(List<LogEntry> raftLog) {
        try {
            Files.createDirectories(logFile.getParent());
            Path tmp = Files.createTempFile(logFile.getParent(), logFile.getFileName().toString(), TMP_SUFFIX);
            try {
                Files.write(tmp, objectMapper.writeValueAsBytes(raftLog));
                fsync(tmp);
                Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to persist Raft log to " + logFile, e);
        }
    }

    public synchronized List<LogEntry> load() {
        if (!Files.exists(logFile)) {
            log.info("Raft log not found at {}, starting with empty log", logFile);
            return List.of();
        }
        try {
            LogEntry[] entries = objectMapper.readValue(Files.readAllBytes(logFile), LogEntry[].class);
            return List.of(entries);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read Raft log from " + logFile, e);
        }
    }

    private static void fsync(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }
}