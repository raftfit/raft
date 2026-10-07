package org.raft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class RaftNode {
    public static void main(String[] args) {
        SpringApplication.run(RaftNode.class, args);
    }
}
