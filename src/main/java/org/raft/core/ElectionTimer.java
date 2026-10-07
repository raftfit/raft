package org.raft.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ElectionTimer {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> scheduledTask;

    public synchronized void reset(Runnable timeoutAction) {
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
        }

        //Нужна для инициализации - чтобы у всех нод не совпали таймеры
        int delay = ThreadLocalRandom.current().nextInt(150, 301);

        scheduledTask = scheduler.schedule(timeoutAction, delay, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
        }
    }
}
