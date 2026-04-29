package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.outbox;

import com.wpanther.saga.domain.outbox.OutboxEvent;
import com.wpanther.saga.domain.outbox.OutboxEventRepository;
import com.wpanther.saga.domain.outbox.OutboxStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
public class JpaOutboxEventRepository implements OutboxEventRepository {

    private static final Logger log = LoggerFactory.getLogger(JpaOutboxEventRepository.class);
    private final SpringDataOutboxRepository springRepository;

    public JpaOutboxEventRepository(SpringDataOutboxRepository springRepository) {
        this.springRepository = springRepository;
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
        return springRepository.save(OutboxEventEntity.fromDomain(event)).toDomain();
    }

    @Override
    public Optional<OutboxEvent> findById(UUID id) {
        return springRepository.findById(id).map(OutboxEventEntity::toDomain);
    }

    @Override
    public List<OutboxEvent> findPendingEvents(int limit) {
        return springRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING, Pageable.ofSize(limit))
                .stream().map(OutboxEventEntity::toDomain).toList();
    }

    @Override
    public List<OutboxEvent> findFailedEvents(int limit) {
        return springRepository.findFailedEventsOrderByCreatedAtAsc(Pageable.ofSize(limit))
                .stream().map(OutboxEventEntity::toDomain).toList();
    }

    @Override
    public int deletePublishedBefore(Instant before) {
        return springRepository.deletePublishedBefore(before);
    }

    @Override
    public List<OutboxEvent> findByAggregate(String aggregateType, String aggregateId) {
        return springRepository.findByAggregateTypeAndAggregateIdOrderByCreatedAtAsc(aggregateType, aggregateId)
                .stream().map(OutboxEventEntity::toDomain).toList();
    }
}
