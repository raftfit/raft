package org.raft.transport;

import org.raft.config.RaftNodeConf;
import org.raft.model.RequestVote;
import org.raft.model.VoteResponse;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Service
public class RaftNodesClient {
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final Map<String, String> peers;
    private final RestClient restClient;


    public RaftNodesClient(RaftNodeConf conf) {
        this.peers = conf.transport().peers();
        this.restClient = buildRestClient(conf);
    }

    private static RestClient buildRestClient(RaftNodeConf conf) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(conf.http().connectTimeoutMs()));
        requestFactory.setReadTimeout(Duration.ofMillis(conf.http().readTimeoutMs()));
        return RestClient.builder().requestFactory(requestFactory).build();
    }

    public void broadcastRequestVote(RequestVote request, Consumer<VoteResponse> callback) {
        var selfId = request.candidateId();

        for (Map.Entry<String, String> entry : peers.entrySet()) {
            String peerId = entry.getKey();
            String url = entry.getValue();

            if (!peerId.equals(selfId)) {
                executor.submit(() -> {
                    try {
                        VoteResponse response = restClient.post()
                                .uri(url + "/api/requestVote")
                                .body(request)
                                .retrieve()
                                .body(VoteResponse.class);

                        if (response != null) {
                            callback.accept(response);
                        }
                    } catch (Exception e) {
                        //error handling
                    }
                });
            }
        }
    }
}
