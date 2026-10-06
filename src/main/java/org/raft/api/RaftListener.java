package org.raft.api;

public interface RaftListener {
    void onElectionTimeout();

    void onVoteResponse(VoteResponse response);

    void onRequestVote(RequestVote request);

    void onHeartbeat(Heartbeat heartbeat);
}
