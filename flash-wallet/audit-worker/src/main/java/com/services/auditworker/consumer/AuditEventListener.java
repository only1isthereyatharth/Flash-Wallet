package com.services.auditworker.consumer;

import com.services.auditworker.service.AuditEventProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AuditEventListener {

    private final AuditEventProcessingService auditEventProcessingService;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @KafkaListener(
            topics = "${audit.worker.topic}",
            containerFactory = "auditKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        log.info(
                "Received audit event from Kafka: groupId={}, topic={}, partition={}, offset={}, key={}",
                consumerGroupId,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key());
        auditEventProcessingService.process(record);
    }
}
