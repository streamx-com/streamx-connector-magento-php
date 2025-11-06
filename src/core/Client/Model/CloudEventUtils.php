<?php

namespace StreamX\ConnectorCore\Client\Model;

use CloudEvents\V1\CloudEventImmutable;
use CloudEvents\V1\CloudEventInterface;
use DateTimeImmutable;
use Ramsey\Uuid\Uuid;

class CloudEventUtils {

    private function __construct() {
    }

    public static function createEvent(string $subject, string $eventType, string $eventSource, Data $data): CloudEventInterface {
        return new CloudEventImmutable(
            Uuid::uuid4()->toString(),
            $eventSource,
            $eventType,
            $data,
            'application/json',
            null,
            $subject,
            new DateTimeImmutable()
        );
    }

    /**
     * @param CloudEventInterface[] $cloudEvents
     * @return string[]
     */
    public static function extractSubjects(array $cloudEvents): array {
        return array_map(fn($event) => $event->getSubject(), $cloudEvents);
    }

    /**
     * @param CloudEventInterface[] $cloudEvents
     * @return string[]
     */
    public static function extractEventTypes(array $cloudEvents): array {
        return array_unique(array_map(fn($event) => $event->getType(), $cloudEvents));
    }
}