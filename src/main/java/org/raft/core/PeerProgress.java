package org.raft.core;

public class PeerProgress {
    long nextIndex;
    long matchIndex;

    PeerProgress(long nextIndex, long matchIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
    }
}