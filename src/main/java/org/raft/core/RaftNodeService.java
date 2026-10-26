package org.raft.core;

import org.raft.config.RaftNodeConf;
import org.raft.model.*;
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

import java.util.*;

@Service
public class RaftNodeService {

    private static final Logger log = LoggerFactory.getLogger(RaftNodeService.class);
    private static final int MAX_ENTRIES_PER_REQUEST = 100;

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
    private long commitedIndx = 0;
    private final Map<String, PeerProgress> peerProgress = new HashMap<>();
    private final Set<String> peerIds;
    private ArrayList<LogEntry> raftLogs = new ArrayList<>();
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
        this.peerIds = Set.copyOf(conf.transport().peers().keySet());
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
            return new HeartbeatResponse(currentTerm, leaderId, false, 0);
        }

        if (req.term() > currentTerm) {
            becomeFollowerOnNewTerm(req.term());
        }

        if (role == Role.LEADER) {
            log.error("[{}] two leaders in term {}", selfId, currentTerm);
            return new HeartbeatResponse(currentTerm, leaderId, false, 0);
        }
        if (role == Role.CANDIDATE) {
            role = Role.FOLLOWER;
            votesReceived.clear();
            log.info("[{}] recognized leader {} in term={}", selfId, req.leaderId(), currentTerm);
        }

        leaderId = req.leaderId();
        timerReset();

        long prevLogIndx = req.prevLogIndex();
        if (prevLogIndx > lastLogIndex()) {
            return new HeartbeatResponse(currentTerm, leaderId, false, lastLogIndex());
        }
        long myPrevTerm = (prevLogIndx == 0) ? 0 : raftLogs.get((int) prevLogIndx - 1).term();
        if (myPrevTerm != req.prevLogTerm()) {
            return new HeartbeatResponse(currentTerm, leaderId, false, lastLogIndex());
        }

        long index = prevLogIndx;
        for (LogEntry entry : req.raftLogs()) {
            index++;
            if (index <= lastLogIndex()) {
                if (raftLogs.get((int) index - 1).term() == entry.term()) {
                    continue;
                }
                raftLogs.subList((int) index - 1, raftLogs.size()).clear();
            }
            raftLogs.add(entry);
        }

        long lastNew = prevLogIndx + req.raftLogs().size();
        long newCommit = Math.min(req.leaderCommit(), lastNew);
        if (newCommit > commitedIndx) {
            commitedIndx = newCommit;
        }

        return new HeartbeatResponse(currentTerm, leaderId, true, lastNew);
    }

    public synchronized void onHeartbeatResponse(String peerId, HeartbeatResponse resp) {
        if (isPaused) return;
        checkTerm(resp.term());
        if (role != Role.LEADER || resp.term() != currentTerm) return;

        PeerProgress peer = peerProgress.get(peerId);
        if (resp.success()) {
            peer.matchIndex = resp.matchIndex();
            peer.nextIndex = resp.matchIndex() + 1;
            updateCommitIndex();
        } else {
            peer.nextIndex = Math.max(1, Math.min(peer.nextIndex - 1, resp.matchIndex() + 1));
        }
    }

    public synchronized ClientEntryResponse appendCommand(String command) {
        if (isPaused) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "node is paused");

        if (role != Role.LEADER) {
            return new ClientEntryResponse(false, 0, currentTerm, leaderId);
        }

        raftLogs.add(new LogEntry(command, currentTerm));
        log.info("[{}] appended entry index={}, term={}", selfId, lastLogIndex(), currentTerm);
        updateCommitIndex();
        return new ClientEntryResponse(true, lastLogIndex(), currentTerm, leaderId);
    }

    private void updateCommitIndex() {
        long n = lastLogIndex();
        while (n > commitedIndx) {
            if (raftLogs.get((int) n - 1).term() != currentTerm) {
                return;
            }
            int count = 1;
            for (PeerProgress p : peerProgress.values()) {
                if (p.matchIndex >= n) {
                    count++;
                }
            }
            if (count >= majority) {
                commitedIndx = n;
                return;
            }
            n--;
        }
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
        for (String peer : peerIds) {
            peerProgress.put(peer, new PeerProgress(lastLogIndex() + 1, 0));
        }
        raftLogs.add(new LogEntry("ELECTION term=" + currentTerm, currentTerm));
        log.info("[{}] became LEADER, term={}, votes={}", selfId, currentTerm, votesReceived);
        updateCommitIndex();
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
        heartbeater.start(this::buildAppendEntries, this::onHeartbeatResponse);
    }

    public synchronized HeartbeatRequest buildAppendEntries (String peerId) {
        if (role != Role.LEADER || isPaused) return null;

        PeerProgress peer = peerProgress.get(peerId);

        long prevLogIndx = peer.nextIndex - 1;
        long prevLogTerm = (prevLogIndx == 0) ? 0 : raftLogs.get((int) prevLogIndx - 1).term();

        int to = (int) Math.min(raftLogs.size(), prevLogIndx + MAX_ENTRIES_PER_REQUEST);
        ArrayList<LogEntry> entries = new ArrayList<>(raftLogs.subList((int) prevLogIndx, to));

        return new HeartbeatRequest(
                currentTerm,
                leaderId,
                prevLogIndx,
                prevLogTerm,
                commitedIndx,
                entries
        );
    }

    private void stopHeartbeat() {
        heartbeater.stop();
    }

    private long lastLogIndex() {
        return raftLogs.size();
    }

    private long lastLogTerm() {
        if (!raftLogs.isEmpty()) {
            return raftLogs.get(raftLogs.size() - 1).term();
        }
        else return 0;
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

    public synchronized long getCommitIndex() {
        return commitedIndx;
    }

    public synchronized long getLastLogIndex() {
        return lastLogIndex();
    }

    public synchronized List<LogEntry> getLog() {
        return List.copyOf(raftLogs);
    }
}