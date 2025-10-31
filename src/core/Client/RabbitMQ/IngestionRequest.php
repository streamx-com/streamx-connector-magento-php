<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client\RabbitMQ;

use CloudEvents\V1\CloudEventInterface;
use StreamX\ConnectorCore\Client\Model\CloudEventsSerializer;

class IngestionRequest {

    /** @var CloudEventInterface[] */
    private array $cloudEvents;
    private bool $isBatch;
    private int $storeId;

    public function __construct(array $cloudEvents, int $storeId) {
        $this->cloudEvents = $cloudEvents;
        $this->isBatch = count($cloudEvents) != 1;
        $this->storeId = $storeId;
    }

    /** @return CloudEventInterface[] */
    public function getCloudEvents(): array {
        return $this->cloudEvents;
    }

    public function isBatch(): bool {
        return $this->isBatch;
    }

    public function getStoreId(): int {
        return $this->storeId;
    }

    public function toJson(): string {
        $result = json_encode([
            'isBatch' => $this->isBatch,
            'storeId' => $this->storeId,
            'cloudEvents' => 'CLOUD_EVENTS'
        ]);

        $serializedEvents = CloudEventsSerializer::serialize($this->cloudEvents);
        return str_replace('"CLOUD_EVENTS"', $serializedEvents, $result);
    }

    public static function fromJson(string $json): IngestionRequest {
        $jsonAsArray = json_decode($json, true);
        $cloudEvents = $jsonAsArray['cloudEvents'];
        $isBatch = $jsonAsArray['isBatch'];
        $storeId = intval($jsonAsArray['storeId']);

        $cloudEvents = CloudEventsSerializer::deserialize(json_encode($cloudEvents), $isBatch);

        return new IngestionRequest($cloudEvents, $storeId);
    }

}