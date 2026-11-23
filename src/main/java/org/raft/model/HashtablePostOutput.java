package org.raft.model;

import tools.jackson.databind.JsonNode;

public record HashtablePostOutput(String key, JsonNode value) {
}
