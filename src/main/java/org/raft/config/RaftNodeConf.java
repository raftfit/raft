package org.raft.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.hibernate.validator.constraints.URL;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.util.Map;

@Validated
@ConfigurationProperties(prefix = "raft")
public record RaftNodeConf(
        @Valid TransportConfig transport,
        @NotBlank String selfId,
        @Positive @DefaultValue("50") long heartbeatIntervalMs,
        @Valid @DefaultValue HttpConfig http,
        @Valid @DefaultValue ElectionConfig election,
        @Valid @DefaultValue StoreConfig store,
        @Valid @DefaultValue EntriesConfig entries
) {

    public record TransportConfig(Map<String, @URL String> peers) {}

    public record HttpConfig(@Positive @DefaultValue("500") long connectTimeoutMs,
                             @Positive @DefaultValue("500") long readTimeoutMs) {}

    public record ElectionConfig(@Positive @DefaultValue("50") int lowerBound,
                                 @Positive @DefaultValue("150") int upperBound) {}

    public record StoreConfig(@NotBlank @DefaultValue("./log") String dir) {}

    public record EntriesConfig(@Positive @DefaultValue("5000") long appendTimeout) { }

    @AssertTrue(message = "Upper bound must be greater or equal than lower bound")
    public boolean isUpperBoundGtOrEqThanLower() {
        if (election == null) return true;
        return election.upperBound() >= election.lowerBound();
    }
}
