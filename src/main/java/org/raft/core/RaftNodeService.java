package org.raft.core;

import org.raft.config.RaftNodeConf;
import org.raft.model.HeartbeatRequest;
import org.raft.model.HeartbeatResponse;
import org.raft.model.RequestVote;
import org.raft.model.VoteResponse;
import org.raft.transport.HeartBeater;
import org.raft.transport.RaftNodesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashSet;
import java.util.Set;

@Service
public class RaftNodeService {

    private static final Logger log = LoggerFactory.getLogger(RaftNodeService.class);

    private final String selfId;

    // Вынести в сетевой уровень - машина состояний не должна это знать сама по себе
    private final int clusterSize;
    private final int majority;

    private final ElectionTimer timer = new ElectionTimer();

    @Autowired
    private HeartBeater heartbeater;

    @Autowired
    private RaftNodesClient client;

    private long currentTerm = 0;
    private String votedFor = null;
    private Role role = Role.FOLLOWER;
    private String leaderId = null;
    private final Set<String> votesReceived = new HashSet<>();

    private boolean isPaused = false;

    public synchronized void pause() {
        isPaused = true;
        timer.stop();
        heartbeater.stop();
    }

    public synchronized void resume() {
        isPaused = false;
        if (role == Role.LEADER) startHeartbeat();
        else timerReset();
    }

    public RaftNodeService(RaftNodeConf conf) {
        this.selfId = conf.selfId();
        this.clusterSize = conf.transport().peers().size() + 1;
        this.majority = clusterSize / 2 + 1;
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void start() {
        log.info("[{}] started as FOLLOWER, term={}", selfId, currentTerm);
        timerReset();
    }

    public synchronized void onElectionTimeout() {
        if (isPaused) return;

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

        timerReset();

        if (votesReceived.size() >= majority) {
            becomeLeader();
            return;
        }

        client.broadcastRequestVote(
                new RequestVote(currentTerm, selfId, lastLogIndex(), lastLogTerm()),
                this::onVoteResponse);
    }

    
    public synchronized VoteResponse onRequestVote(RequestVote req) {
        if (isPaused) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "node is paused");

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
            timerReset();
            reason = "vote granted";
        }

        log.info("[{}] RequestVote from {} (term={}): {}", selfId, req.candidateId(), req.term(), reason);
        return new VoteResponse(currentTerm, selfId, grant);
    }

    
    public synchronized void onVoteResponse(VoteResponse resp) {
        if (isPaused) return;

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

    public synchronized HeartbeatResponse onHeartbeat(HeartbeatRequest req) {
        if (isPaused) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "node is paused");

        if (req.term() < currentTerm) {
            return new HeartbeatResponse(currentTerm, leaderId);
        }

        if (req.term() > currentTerm) {
            becomeFollowerOnNewTerm(req.term());
        }

        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            votesReceived.clear();
            log.info("[{}] recognized leader {} in term={}", selfId, req.leaderId(), currentTerm);
        }

        leaderId = req.leaderId();
        timerReset();

        return new HeartbeatResponse(currentTerm, leaderId);
    }

    public synchronized void onHeartbeatResponse(HeartbeatResponse resp) {
        if (isPaused) return;
        checkTerm(resp.term());
    }

    private void checkTerm(long term) {
        if (term > currentTerm) {
            becomeFollowerOnNewTerm(term);
        }
    }

    private void becomeFollowerOnNewTerm(long term) {
        boolean wasLeader = role == Role.LEADER;
        currentTerm = term;
        votedFor = null;
        role = Role.FOLLOWER;
        leaderId = null;
        votesReceived.clear();
        if (wasLeader) {
            stopHeartbeat();
            timerReset();
        }
        log.info("[{}] became FOLLOWER, term={}", selfId, term);
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = selfId;
        timer.stop();
        log.info("[{}] became LEADER, term={}, votes={}", selfId, currentTerm, votesReceived);
        startHeartbeat();
    }

    private boolean logIsUpToDate(long candidateLastIndex, long candidateLastTerm) {
        if (candidateLastTerm != lastLogTerm()) {
            return candidateLastTerm > lastLogTerm();
        }
        return candidateLastIndex >= lastLogIndex();
    }

    private void timerReset() {
        timer.reset(this::onElectionTimeout);
    }

    private void startHeartbeat() {
        heartbeater.start(this::onHeartbeatResponse, new HeartbeatRequest(
                currentTerm,
                leaderId,
                lastLogIndex(),
                lastLogTerm()
        ));
    }

    private void stopHeartbeat() {
        heartbeater.stop();
    }

    // Лог будет добавлен в следующих заданиях
    private long lastLogIndex() {
        return 0;
    }

    private long lastLogTerm() {
        return 0;
    }

    public synchronized String getSelfId() {
        return selfId;
    }

    public synchronized String getVotedFor() {
        return votedFor;
    }

    public synchronized String getLeaderId() {
        return leaderId;
    }

    public synchronized Role getRole() {
        return role;
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized boolean getIsPaused() {
        return isPaused;
    }
}