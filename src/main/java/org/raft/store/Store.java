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

/**
 * Персистентность всего durable-состояния Raft-ноды: currentTerm, votedFor и лог.
 * Пишется одним атомарным JSON-файлом (temp + fsync + ATOMIC_MOVE), чтобы на диске
 * всегда была либо старая, либо новая согласованная тройка — никогда не обрыв.
 */
@Component
public class Store {

    private static final Logger log = LoggerFactory.getLogger(Store.class);
    private static final String FILE_SUFFIX = "-raft-state.json";
    private static final String TMP_SUFFIX = ".tmp";

    @Autowired
    private ObjectMapper objectMapper;
    private final Path stateFile;

    public Store(RaftNodeConf conf) {
        this.stateFile = Paths.get(conf.store().dir()).resolve(conf.selfId() + FILE_SUFFIX);
    }

    public Path getStateFile() {
        return stateFile;
    }

    public synchronized void save(long currentTerm, String votedFor, List<LogEntry> raftLog) {
        try {
            Files.createDirectories(stateFile.getParent());
            Path tmp = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), TMP_SUFFIX);
            try {
                Files.write(tmp, objectMapper.writeValueAsBytes(new RaftState(currentTerm, votedFor, raftLog)));
                fsync(tmp);
                Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to persist Raft state to " + stateFile, e);
        }
    }

    /**
     * Читает состояние с диска. Если файла ещё нет — стартовое состояние (терм 0, голоса нет, пустой лог).
     * Повреждённый файл — критическое состояние, бросаем исключение, а не молча стартуем с нуля.
     */
    public synchronized RaftState load() {
        if (!Files.exists(stateFile)) {
            log.info("Raft state not found at {}, starting fresh", stateFile);
            return RaftState.empty();
        }
        try {
            return objectMapper.readValue(Files.readAllBytes(stateFile), RaftState.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read Raft state from " + stateFile, e);
        }
    }

    /** Durable-состояние ноды, которое Raft обязан персистить: терм, голос и лог. */
    public record RaftState(long currentTerm, String votedFor, List<LogEntry> raftLog) {

        public static RaftState empty() {
            return new RaftState(0, null, List.of());
        }
    }

    private static void fsync(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }
}