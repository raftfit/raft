package org.raft.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "raft")
public record RaftNodeConf(
        TransportConfig transport,
        String selfId,
        long heartbeatIntervalMs,
        HttpConfig http
) {

    public record TransportConfig(Map<String, String> peers) {}

    public record HttpConfig(long connectTimeoutMs, long readTimeoutMs) {}
}
