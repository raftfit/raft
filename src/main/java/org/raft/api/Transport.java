package org.raft.api;

public interface Transport {
    void broadcastRequestVote(RequestVote request);
    void sendVoteResponse(String to, VoteResponse response);
    void startHeartbeat(long term);
    void stopHeartbeat();
    void resetElectionTimer();
    void setRaftListener(RaftListener listener);
}
