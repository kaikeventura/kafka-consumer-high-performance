package com.highperformance.kafka.service;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SqsProducerService {

    private static final Logger log = LoggerFactory.getLogger(SqsProducerService.class);

    private final SqsAsyncClient sqsAsyncClient;
    private final String queueUrl;
    private final int batchSize;
    private final ConcurrentLinkedQueue<String> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger(0);

    public SqsProducerService(SqsAsyncClient sqsAsyncClient,
                              @Value("${sqs.queue-name}") String queueName,
                              @Value("${aws.endpoint-url}") String endpointUrl,
                              @Value("${sqs.batch-size:100}") int batchSize) {
        this.sqsAsyncClient = sqsAsyncClient;
        this.queueUrl = endpointUrl + "/000000000000/" + queueName;
        this.batchSize = batchSize;
    }

    @Async
    public void sendMessage(String messageBody) {
        buffer.offer(messageBody);
        pendingCount.incrementAndGet();

        if (buffer.size() >= batchSize) {
            flushBatch();
        }
    }

    @Scheduled(fixedDelay = 100)
    public void scheduledFlush() {
        if (!buffer.isEmpty()) {
            flushBatch();
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Flushing {} pending messages to SQS", pendingCount.get());
        while (!buffer.isEmpty()) {
            flushBatch();
        }
    }

    private void flushBatch() {
        List<String> messages = new ArrayList<>();
        while (!buffer.isEmpty()) {
            String msg = buffer.poll();
            if (msg != null) {
                messages.add(msg);
            }
        }

        if (messages.isEmpty()) {
            return;
        }

        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            // SQS batch limit is 10
            for (int i = 0; i < messages.size(); i += 10) {
                int end = Math.min(i + 10, messages.size());
                List<String> chunk = messages.subList(i, end);

                List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
                for (int j = 0; j < chunk.size(); j++) {
                    entries.add(SendMessageBatchRequestEntry.builder()
                            .id(String.valueOf(j))
                            .messageBody(chunk.get(j))
                            .build());
                }

                SendMessageBatchRequest request = SendMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(entries)
                        .build();

                // Send in parallel using async client
                futures.add(CompletableFuture.runAsync(() -> {
                    sqsAsyncClient.sendMessageBatch(request).join();
                }));
            }

            // Wait for all batches to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            pendingCount.addAndGet(-messages.size());
            log.debug("Batch sent to SQS: {} messages ({} batches)", messages.size(), futures.size());
        } catch (Exception e) {
            log.error("Error sending batch to SQS: {}", e.getMessage(), e);
            messages.forEach(buffer::offer);
            pendingCount.addAndGet(messages.size());
        }
    }

    public int getPendingCount() {
        return pendingCount.get();
    }
}
