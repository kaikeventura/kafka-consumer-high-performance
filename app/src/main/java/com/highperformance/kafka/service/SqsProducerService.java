package com.highperformance.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

@Service
public class SqsProducerService {

    private static final Logger log = LoggerFactory.getLogger(SqsProducerService.class);

    private final SqsClient sqsClient;
    private final String queueUrl;

    public SqsProducerService(SqsClient sqsClient,
                              @Value("${sqs.queue-name}") String queueName,
                              @Value("${aws.endpoint-url}") String endpointUrl) {
        this.sqsClient = sqsClient;
        this.queueUrl = endpointUrl + "/000000000000/" + queueName;
    }

    @Async
    public void sendMessage(String messageBody) {
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        SendMessageResponse response = sqsClient.sendMessage(request);
        log.debug("Message sent to SQS: messageId={}", response.messageId());
    }
}
