<?php

namespace StreamX\ConnectorCore\Client\Model;

use CloudEvents\Serializers\JsonDeserializer;
use CloudEvents\Serializers\JsonSerializer;
use CloudEvents\V1\CloudEventInterface;
use Exception;
use TypeError;

class CloudEventsSerializer {

    private static ?JsonSerializer $jsonSerializer = null;
    private static ?JsonDeserializer $jsonDeserializer = null;

    /**
     * @param CloudEventInterface[] $cloudEvents
     */
    public static function serialize(array $cloudEvents): string {
        try {
            $jsonSerializer = self::getSerializer();
            if (count($cloudEvents) == 1) {
                return $jsonSerializer->serializeStructured($cloudEvents[0]);
            } else {
                return $jsonSerializer->serializeBatch($cloudEvents);
            }
        } catch (TypeError $e) {
            // bug in JsonSerializer: internally it uses json_encode(), and that returns false (not a string) on deserialization error
            $errorMessage = 'Serialization error';
            $cause = json_last_error() === JSON_ERROR_NONE ? '' : ': ' . json_last_error_msg();
            throw new Exception($errorMessage . $cause);
        }
    }

    /**
     * @return CloudEventInterface[]
     */
    public static function deserialize(string $json, bool $isBatch): array {
        $jsonDeserializer = self::getDeserializer();
        if (!$isBatch) {
            return [$jsonDeserializer->deserializeStructured($json)];
        } else {
            return $jsonDeserializer->deserializeBatch($json);
        }
    }

    private static function getSerializer(): JsonSerializer {
        return self::$jsonSerializer ??= JsonSerializer::create();
    }

    private static function getDeserializer(): JsonDeserializer {
        return self::$jsonDeserializer ??= JsonDeserializer::create();
    }
}