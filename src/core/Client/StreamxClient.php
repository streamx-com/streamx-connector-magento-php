<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client;

use CloudEvents\V1\CloudEventInterface;
use Exception;
use Magento\Store\Api\Data\StoreInterface;
use Psr\Log\LoggerInterface;
use StreamX\ConnectorCore\Client\Model\CloudEventUtils;
use StreamX\ConnectorCore\Client\Model\Data;
use StreamX\ConnectorCore\Client\RabbitMQ\IngestionRequest;
use StreamX\ConnectorCore\Client\RabbitMQ\RabbitMqConfiguration;
use StreamX\ConnectorCore\Client\RabbitMQ\RabbitMqIngestionRequestsSender;
use StreamX\ConnectorCore\Traits\ExceptionLogger;

class StreamxClient {
    use ExceptionLogger;

    private LoggerInterface $logger;
    private StreamxClientConfiguration $streamxClientConfiguration;
    private RabbitMqConfiguration $rabbitMqConfiguration;
    private RabbitMqIngestionRequestsSender $rabbitMqSender;
    private StreamxIngestor $streamxIngestor;

    public function __construct(
        LoggerInterface $logger,
        StreamxClientConfiguration $streamxClientConfiguration,
        RabbitMqConfiguration $rabbitMqConfiguration,
        RabbitMqIngestionRequestsSender $rabbitMqSender,
        StreamxIngestor $streamxIngestor
    ) {
        $this->logger = $logger;
        $this->streamxClientConfiguration = $streamxClientConfiguration;
        $this->rabbitMqConfiguration = $rabbitMqConfiguration;
        $this->rabbitMqSender = $rabbitMqSender;
        $this->streamxIngestor = $streamxIngestor;
    }

    public function publish(array $entities, string $indexerId, StoreInterface $store): void {
        $this->createEventsAndIngest(true, $entities, $indexerId, $store);
    }

    public function unpublish(array $entityIds, string $indexerId, StoreInterface $store): void {
        $entities = array_map(fn($id) => ['id' => (string) $id], $entityIds);
        $this->createEventsAndIngest(false, $entities, $indexerId, $store);
    }

    public function createEventsAndIngest(bool $isPublish, array $entities, string $indexerId, StoreInterface $store): void {
        $storeId = (int)$store->getId();
        $eventSource = $this->streamxClientConfiguration->getEventSource($storeId);
        $eventType = $isPublish
            ? $this->streamxClientConfiguration->getPublishingEventType($storeId)
            : $this->streamxClientConfiguration->getUnpublishingEventType($storeId);

        $events = [];
        foreach ($entities as $entity) {
            $content = $isPublish
                ? json_encode($entity)
                : null;
            $entityType = $isPublish
                ? EntityType::fromEntityAndIndexerId($entity, $indexerId)
                : EntityType::fromIndexerId($indexerId);
            $key = $this->createStreamxKey($entityType, $entity['id'], $store);
            $data = new Data($content, $entityType->getFullyQualifiedName());
            $events[] = CloudEventUtils::createEvent($key, $eventType, $eventSource, $data);
        }
        $this->ingest($events, $eventType, $indexerId, $store);
    }

    private function createStreamxKey(EntityType $entityType, string $entityId, StoreInterface $store): string {
        return sprintf('%s_%s:%d',
            $store->getCode(),
            $entityType->getRootType(),
            $entityId
        );
    }

    /**
     * @param CloudEventInterface[] $cloudEvents
     */
    protected function ingest(array $cloudEvents, string $eventType, string $indexerId, StoreInterface $store): void {
        $keys = CloudEventUtils::extractSubjects($cloudEvents);
        $eventsCount = count($cloudEvents);
        $this->logger->info("Start sending $eventsCount $eventType entities from $indexerId with keys " . json_encode($keys));
        $storeId = (int) $store->getId();

        try {
            if ($this->rabbitMqConfiguration->isEnabled()) {
                $this->rabbitMqSender->send(new IngestionRequest($cloudEvents, $storeId));
            } else {
                $this->streamxIngestor->send($cloudEvents, $storeId);
            }
        } catch (Exception $e) {
            $this->logExceptionAsError('Event sending exception', $e);
        }

        $this->logger->info("Finished sending $eventsCount $eventType entities from $indexerId");
    }
}
