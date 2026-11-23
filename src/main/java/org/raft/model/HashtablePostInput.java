package org.raft.model;

import jakarta.validation.constraints.NotBlank;
import tools.jackson.databind.JsonNode;

public record HashtablePostInput(@NotBlank String key, JsonNode value) {
}
