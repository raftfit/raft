package org.raft.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.raft.api.Heartbeat;
import org.raft.api.RequestVote;
import org.raft.api.VoteResponse;
import org.raft.api.RaftListener;
import org.raft.api.Transport;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RaftNodeService implements RaftListener {

    private static final Logger log = LoggerFactory.getLogger(RaftNodeService.class);

    private final String selfId;
    private final List<String> peers;
    private final int clusterSize;
    private final int majority;
    private final Transport transport;

    private long currentTerm = 0;
    private String votedFor = null;
    private Role role = Role.FOLLOWER;
    private String leaderId = null;
    private final Set<String> votesReceived = new HashSet<>();

    public RaftNodeService(String selfId, List<String> peers, Transport transport) {
        this.selfId = selfId;
        this.peers = List.copyOf(peers);
        this.clusterSize = this.peers.size() + 1;
        this.majority = clusterSize / 2 + 1;
        this.transport = transport;
        transport.setRaftListener(this);
    }

    @Override
    public synchronized void onElectionTimeout() {
        if (role == Role.LEADER) {
            return;
        }

        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = selfId;
        leaderId = null;
        votesReceived.clear();
        votesReceived.add(selfId);
        log.info("[{}] election timeout, starting election, term={}", selfId, currentTerm);

        transport.resetElectionTimer();

        if (votesReceived.size() >= majority) {
            becomeLeader();
            return;
        }

        transport.broadcastRequestVote(
                new RequestVote(currentTerm, selfId, lastLogIndex(), lastLogTerm()));
    }

    @Override
    public synchronized void onRequestVote(RequestVote req) {
        checkTerm(req.term());

        boolean grant;
        String reason;
        if (req.term() < currentTerm) {
            grant = false;
            reason = "stale term";
        } else if (votedFor != null && !votedFor.equals(req.candidateId())) {
            grant = false;
            reason = "already voted for " + votedFor;
        } else if (!logIsUpToDate(req.lastLogIndex(), req.lastLogTerm())) {
            grant = false;
            reason = "candidate's log is out of date";
        } else {
            grant = true;
            votedFor = req.candidateId();
            transport.resetElectionTimer();
            reason = "vote granted";
        }

        log.info("[{}] RequestVote from {} (term={}): {}", selfId, req.candidateId(), req.term(), reason);
        transport.sendVoteResponse(req.candidateId(), new VoteResponse(currentTerm, selfId, grant));
    }

    @Override
    public synchronized void onVoteResponse(VoteResponse resp) {
        checkTerm(resp.term());

        if (role != Role.CANDIDATE) {
            return;
        }
        if (resp.term() != currentTerm) {
            log.debug("[{}] stale response from {} (term={}), ignoring", selfId, resp.voterId(), resp.term());
            return;
        }
        if (!resp.voteGranted()) {
            log.info("[{}] vote denied by {}", selfId, resp.voterId());
            return;
        }

        votesReceived.add(resp.voterId());
        log.info("[{}] vote from {}, total {}/{}", selfId, resp.voterId(), votesReceived.size(), clusterSize);

        if (votesReceived.size() >= majority) {
            becomeLeader();
        }
    }

    @Override
    public synchronized void onHeartbeat(Heartbeat hb) {
        checkTerm(hb.term());

        if (hb.term() < currentTerm) {
            return;
        }

        if (role == Role.CANDIDATE) {
            role = Role.FOLLOWER;
            votesReceived.clear();
            log.info("[{}] recognized leader {} in term={}", selfId, hb.leaderId(), currentTerm);
        }

        if (role == Role.LEADER) {
            log.error("[{}] ERROR: second leader {} in term={}", selfId, hb.leaderId(), currentTerm);
            return;
        }

        leaderId = hb.leaderId();
        transport.resetElectionTimer();
    }

    public synchronized Status getStatus() {
        return new Status(selfId, role, currentTerm, votedFor, leaderId, peers);
    }

    private void checkTerm(long term) {
        if (term > currentTerm) {
            becomeFollower(term);
        }
    }

    private void becomeFollower(long term) {
        boolean wasLeader = role == Role.LEADER;
        currentTerm = term;
        votedFor = null;
        role = Role.FOLLOWER;
        leaderId = null;
        votesReceived.clear();
        if (wasLeader) {
            transport.stopHeartbeat();
        }
        log.info("[{}] became FOLLOWER, term={}", selfId, term);
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = selfId;
        log.info("[{}] became LEADER, term={}, votes={}", selfId, currentTerm, votesReceived);
        transport.startHeartbeat(currentTerm);
    }

    private boolean logIsUpToDate(long candidateLastIndex, long candidateLastTerm) {
        if (candidateLastTerm != lastLogTerm()) {
            return candidateLastTerm > lastLogTerm();
        }
        return candidateLastIndex >= lastLogIndex();
    }

    private long lastLogIndex() {
        return 0;
    }

    private long lastLogTerm() {
        return 0;
    }
}