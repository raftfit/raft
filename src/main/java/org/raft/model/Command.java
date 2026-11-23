package org.raft.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NoOp.class, name = "noop"),
        @JsonSubTypes.Type(value = HashtablePost.class, name = "post")
})
public sealed interface Command permits NoOp, HashtablePost {
}
