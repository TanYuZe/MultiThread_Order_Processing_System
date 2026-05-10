package com.platform.common.logging;

import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;

import java.util.Map;

/**
 * Propagates MDC context (correlationId, threadType, etc.) from the parent
 * thread to child threads spawned by async executors.
 *
 * THE PROBLEM this solves:
 *   ThreadLocal (which MDC uses internally) is NOT inherited by child threads.
 *   Without this decorator:
 *     - Parent thread (Tomcat): correlationId = "abc-123"
 *     - Child thread (@Async / CompletableFuture): correlationId = null
 *
 *   This means async log lines have no correlation ID — impossible to trace
 *   what request triggered which async operation.
 *
 * Register this decorator in every ThreadPoolTaskExecutor:
 *   executor.setTaskDecorator(new MdcTaskDecorator());
 *
 * This does NOT automatically propagate to ForkJoinPool or raw Thread.
 * For those, manually copy MDC context before submitting tasks.
 */
public class MdcTaskDecorator implements TaskDecorator {

    @Override
    public Runnable decorate(Runnable runnable) {
        // Capture MDC context from the parent thread at submission time
        Map<String, String> contextMap = MDC.getCopyOfContextMap();

        return () -> {
            if (contextMap != null) {
                MDC.setContextMap(contextMap);  // restore in child thread
            }
            try {
                runnable.run();
            } finally {
                MDC.clear();  // prevent context leak when thread returns to pool
            }
        };
    }
}
