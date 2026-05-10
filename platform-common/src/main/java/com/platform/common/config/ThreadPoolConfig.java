package com.platform.common.config;

import com.platform.common.logging.MdcTaskDecorator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Centralized thread pool configuration.
 *
 * LEARNING FOCUS:
 *   - MdcTaskDecorator ensures correlationId propagates to child threads
 *   - Queue capacity is deliberately observable — when queue fills,
 *     CallerRunsPolicy applies backpressure to the submitting thread
 *   - Thread name prefix appears in logs and thread dumps — use it!
 *
 * Compare thread dump output during load test:
 *   BEFORE tuning: "http-nio-8081-exec-N" threads all stuck in same stack frame
 *   AFTER separate pools: "payment-async-N" and "notif-async-N" are independent
 */
@Configuration
public class ThreadPoolConfig {

    @Bean("asyncExecutor")
    public Executor asyncExecutor(
            @Value("${spring.task.execution.pool.core-size:10}") int coreSize,
            @Value("${spring.task.execution.pool.max-size:50}") int maxSize,
            @Value("${spring.task.execution.pool.queue-capacity:100}") int queueCapacity,
            @Value("${spring.task.execution.pool.thread-name-prefix:async-}") String prefix) {
        var executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(coreSize);
        executor.setMaxPoolSize(maxSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix(prefix);
        executor.setTaskDecorator(new MdcTaskDecorator());
        // CallerRunsPolicy: when queue is full, the CALLING thread executes the task.
        // This provides backpressure — the HTTP thread slows down, preventing OOM.
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }
}
