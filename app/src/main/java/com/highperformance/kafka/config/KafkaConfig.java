package com.highperformance.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

@Configuration
public class KafkaConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "${spring.kafka.bootstrap-servers}");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "${spring.kafka.consumer.group-id}");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "${spring.kafka.consumer.auto-offset-reset}");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "${spring.kafka.consumer.properties.max.poll.records}");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "${spring.kafka.consumer.properties.fetch.min.bytes}");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "${spring.kafka.consumer.properties.fetch.max.wait.ms}");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setBatchListener(true);
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency("${spring.kafka.listener.concurrency}");
        return factory;
    }
}
