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

    public function getFormattedPublishedKeys(): string {
        return self::getFormattedKeys($this->publishedKeys);
    }

    public function getFormattedUnpublishedKeys(): string {
        return self::getFormattedKeys($this->unpublishedKeys);
    }

    private static function getFormattedKeys(array $keys) : string {
        if (empty($keys)) {
            return '-';
        }

        $idsByPrefix = self::toIdsByPrefixMap(array_unique($keys));

        $result = '';
        foreach ($idsByPrefix as $prefix => $ids) {
            sort($ids);
            $result .= ("\n - $prefix: " . implode(', ', $ids));
        }
        return $result;
    }

    private static function toIdsByPrefixMap(array $keys): array {
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