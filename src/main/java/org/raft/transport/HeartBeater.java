package org.raft.transport;

import org.raft.config.RaftNodeConf;
import org.raft.model.HeartbeatRequest;
import org.raft.model.HeartbeatResponse;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Service
public class HeartBeater {
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> scheduledTask;
    private final Map<String, String> peers;
    private final RestClient restClient;
    private final long intervalMs;

    public HeartBeater(RaftNodeConf conf) {
        this.peers = conf.transport().peers();
        this.restClient = buildRestClient(conf);
        this.intervalMs = conf.heartbeatIntervalMs();
    }

    private static RestClient buildRestClient(RaftNodeConf conf) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(conf.http().connectTimeoutMs()));
        requestFactory.setReadTimeout(Duration.ofMillis(conf.http().readTimeoutMs()));
        return RestClient.builder().requestFactory(requestFactory).build();
    }

    public synchronized void start(Consumer<HeartbeatResponse> callback, HeartbeatRequest request) {
        stop();

        scheduledTask = scheduler.scheduleAtFixedRate(
                () -> sendHeartBeat(callback, request),
                0,
                intervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    public synchronized void stop() {
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
            scheduledTask = null;
        }
    }

    private void sendHeartBeat(Consumer<HeartbeatResponse> callback, HeartbeatRequest request) {
        for (Map.Entry<String, String> entry : peers.entrySet()) {
            String url = entry.getValue();

            executor.submit(() -> {
                try {
                    HeartbeatResponse response = restClient.post()
                            .uri(url + "/api/heartbeat")
                            .body(request)
                            .retrieve()
                            .body(HeartbeatResponse.class);

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
