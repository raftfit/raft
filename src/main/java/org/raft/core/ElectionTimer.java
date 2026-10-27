package org.raft.core;

import org.raft.config.RaftNodeConf;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ElectionTimer {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> scheduledTask;
    private int upperBound, lowerBound;


    public ElectionTimer(RaftNodeConf conf) {
        upperBound = conf.election().upperBound();
        lowerBound = conf.election().lowerBound();
    }

    public synchronized void reset(Runnable timeoutAction) {
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
        }

        //Нужна для инициализации - чтобы у всех нод не совпали таймеры
        int delay = ThreadLocalRandom.current().nextInt(lowerBound, upperBound);

        scheduledTask = scheduler.schedule(timeoutAction, delay, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
        }
    }
}
