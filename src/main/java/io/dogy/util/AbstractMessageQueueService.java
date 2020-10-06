package io.dogy.util;

import com.fasterxml.jackson.databind.JavaType;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.media.sound.InvalidDataException;
import net.jodah.failsafe.*;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractMessageQueueService<T> implements IStatusMonitor {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final JavaType targetClass;
    private boolean debug = true;
    private RocksDB backupDB;
    private final String name = this.getClass().getSimpleName();

    private final ThreadPoolExecutor scheduler;
    private final FailsafeExecutor<Message<T>> executor;

    private final AtomicLong notCompletedCount = new AtomicLong(0);
    private final FrequencyCounter frequencyCounter = new FrequencyCounter(5, TimeUnit.MINUTES);

    private IFailedMessageHandler failedMessageHandler;

    @Override
    public CurrentStatusInfo getCurrentStatusInfo() {
        CurrentStatusInfo status = new CurrentStatusInfo();
        status.setName(this.getName());
        status.setActive(scheduler.getActiveCount());
        status.setQueued(scheduler.getQueue().size());
        status.setNotCompleted(notCompletedCount.get());
        status.setFrequency(frequencyCounter.getCount());

        return status;
    }

    public AbstractMessageQueueService(Builder<T> builder) {
        this.targetClass = builder.targetClass;
        this.scheduler = builder.scheduler;
        scheduler.setThreadFactory(new ThreadFactoryBuilder().setNameFormat(name + "-%d").build());
        scheduler.allowCoreThreadTimeOut(true);

        this.executor = Failsafe.with(builder.policies).with(scheduler);

        try {
            // a static method that loads the RocksDB C++ library.
            RocksDB.loadLibrary();

            final Options options = new Options().setCreateIfMissing(true);
            String ROCKS_DB_DIR = "Data";
            String path = ROCKS_DB_DIR + "/" + this.getName();
            FileUtils.forceMkdir(new File(path));

            this.backupDB = RocksDB.open(options, path);
        } catch (RocksDBException | IOException e) {
            logger.error("Error when open RocksDB: ", e);
            System.exit(1);
        }
    }

    public void handleMessage(Message<T> originalMessage) throws Exception {
        originalMessage.setServiceName(this.getName());
        assert targetClass.hasRawClass(originalMessage.getPayload().getClass());

        final String key = originalMessage.getKey();
        byte[] bytes = ByteUtil.toBytes(originalMessage);
        backupDB.put(ByteUtil.toBytes(key), bytes);

        // clone message for immutable purpose
        Message<T> message = ByteUtil.toPOJO(bytes, targetClass);

        notCompletedCount.incrementAndGet();
        frequencyCounter.increment();

        executor.runAsync(context -> {
            try {
                if (message.getTimes() == null) {
                    message.setTimes(context.getAttemptCount());
                }
                processMessage(message);
            } catch (InvalidDataException e) {
                if (debug) {
                    logger.warn("[WARN] Invalid Data: " + Util.OBJECT_MAPPER.writeValueAsString(message.getPayload()), e);
                }
            } catch (Exception e) {
                if (debug) {
                    logger.error("[ERROR]: " + Util.OBJECT_MAPPER.writeValueAsString(message.getPayload()), e);
                }
                throw e;
            }
        }).whenComplete((result, ex) -> {
            try {
                backupDB.delete(ByteUtil.toBytes(key));
                notCompletedCount.decrementAndGet();
                if (ex == null) {
                    return;
                }
                if (failedMessageHandler != null) {
                    failedMessageHandler.insert(message);
                }
            } catch (Exception e) {
                logger.error("Uncaught Exception", ex);
            }
        });
    }

    protected abstract void processMessage(Message<T> message) throws Exception;

    public void loadOldNotProcessedMessages() {
        try (RocksIterator iterator = this.backupDB.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                try {
                    Message<T> message = ByteUtil.toPOJO(iterator.value(),
                            Util.OBJECT_MAPPER.getTypeFactory().constructParametricType(Message.class, targetClass));
                    handleMessage(message);
                } catch (Exception e) {
                    logger.error("Error when loading old messages from last downtime: ", e);
                }
                iterator.next();
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public void setFailedMessageHandler(IFailedMessageHandler failedMessageHandler) {
        this.failedMessageHandler = failedMessageHandler;
    }

    public static class Builder<T> {

        private final JavaType targetClass;
        private ThreadPoolExecutor scheduler = new ThreadPoolExecutor(
                10, 10,
                10, TimeUnit.SECONDS, new LinkedBlockingQueue<>()
        );
        private final RetryPolicy<Message<T>> retryPolicy = new RetryPolicy<Message<T>>()
                .withBackoff(1, 600, ChronoUnit.SECONDS, 3.5)
                .withMaxDuration(Duration.ofDays(1));

        private final CircuitBreaker<Message<T>> breaker = new CircuitBreaker<Message<T>>()
                // Breaker will be configured to open when fail 20% (if minimum 5 request) in 1 minute
                .withFailureRateThreshold(20, 5, Duration.ofMinutes(1))
                // After opening, a breaker will delay for 1 minute by default before before transitioning to half-open
                .withDelay(Duration.ofSeconds(30))
                // The breaker can be configured to close again if a number of trial executions succeed, else it will re-open
                .withSuccessThreshold(5);

        private List<Policy<Message<T>>> policies = Arrays.asList(retryPolicy, breaker);

        public Builder(Class<T> targetClass) {
            assert targetClass != null;
            this.targetClass = Util.OBJECT_MAPPER.constructType(targetClass);
        }

        public Builder(JavaType targetClass) {
            assert targetClass != null;
            this.targetClass = targetClass;
        }

        public Builder<T> withScheduler(ThreadPoolExecutor scheduler) {
            assert scheduler != null;
            this.scheduler = scheduler;
            return this;
        }

        public Builder<T> withRetryPolicy(Policy<Message<T>>... policies) {
            this.policies = Arrays.asList(policies);
            return this;
        }

    }

}

