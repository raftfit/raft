package org.raft.model;

import tools.jackson.databind.JsonNode;

public record HashtablePost(String key, JsonNode value) implements Command {
}
