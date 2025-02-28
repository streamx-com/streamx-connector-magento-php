<?php declare(strict_types = 1);

namespace StreamX\ConnectorCatalog\test\integration\utils;

class IngestedKeys {

    private array $publishedKeys = [];
    private array $unpublishedKeys = [];

    public function addPublishedKeys(array $keys): void {
        array_push($this->publishedKeys, ...$keys);
    }

    public function addUnpublishedKeys(array $keys): void {
        array_push($this->unpublishedKeys, ...$keys);
    }

    public function formatted(): string {
        $resultLines = [];
        if (!empty($this->publishedKeys)) {
            array_push($resultLines, ...self::getFormattedKeys($this->publishedKeys, ' + '));
        }
        if (!empty($this->unpublishedKeys)) {
            array_push($resultLines, ...self::getFormattedKeys($this->unpublishedKeys, ' - '));
        }
        return implode("\n", $resultLines);
    }

    private static function getFormattedKeys(array $keys, string $linePrefix) : array {
        $idsByPrefix = self::toIdsByKeyPrefixMap(array_unique($keys));

        $resultLines = [];
        foreach ($idsByPrefix as $keyPrefix => $ids) {
            sort($ids);
            $resultLines[] =  "$linePrefix$keyPrefix: " . implode(', ', $ids);
        }
        return $resultLines;
    }

    private static function toIdsByKeyPrefixMap(array $keys): array {
        $idsByPrefix = [];
        foreach ($keys as $key) {
            $parts = explode(':', str_replace('"', '', $key));
            $prefix = $parts[0];
            $id = $parts[1];
            $idsByPrefix[$prefix][] = $id;
        }
        return $idsByPrefix;
    }
}