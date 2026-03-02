<?php

namespace StreamX\ConnectorCatalog\test\integration\utils;

use PHPUnit\Framework\ExpectationFailedException;

trait ValidationFileUtils  {

    public static function readValidationFileContent(string $validationFileName): string {
        $validationFilesDir = FileUtils::findFolder('resources/validation');
        return file_get_contents("$validationFilesDir/$validationFileName");
    }

    public function verifySameJsonsOrThrow(string $expectedJson, string $actualJson, array $regexReplacements = []): void {
        $this->verifySameJsons($expectedJson, $actualJson, true, $regexReplacements);
    }

    public function verifySameJsonsSilently(string $expectedJson, string $actualJson, array $regexReplacements = []): bool {
        return $this->verifySameJsons($expectedJson, $actualJson, false, $regexReplacements);
    }

    private function verifySameJsons(string $expectedJson, string $actualJson, bool $throwOnAssertionError, array $regexReplacements = []): bool {
        try {
            $expectedJson = self::adjustExpectedJson($expectedJson);
            $actualJson = self::adjustActualJson($actualJson, $regexReplacements);
            $this->assertEquals($expectedJson, $actualJson);
            return true;
        } catch (ExpectationFailedException $e) {
            if ($throwOnAssertionError) {
                throw $e;
            }
            return false;
        }
    }

    private function adjustExpectedJson(string $json): string {
        $jsonArray = json_decode($json, true);
        return self::toNormalizedJson($jsonArray);
    }

    private function adjustActualJson(string $json, array $regexReplacements = []): string {
        $json = self::replaceRegexes($json, $regexReplacements);
        $json = self::standardizeNewlines($json);

        $jsonArray = json_decode($json, true);
        self::removeFieldsAddedByOpensearchWrapper($jsonArray);
        return self::toNormalizedJson($jsonArray);
    }

    private function standardizeNewlines(string $json): string {
        return str_replace('\r\n', '\n', $json);
    }

    private function replaceRegexes(string $json, array $regexReplacements): string {
        foreach ($regexReplacements as $regex => $replacement) {
            $json = preg_replace("|$regex|m", $replacement, $json);
        }
        return $json;
    }

    private static function toNormalizedJson(array $jsonArray): string {
        self::normalizeArray($jsonArray);
        return json_encode($jsonArray, JSON_PRETTY_PRINT);
    }

    /**
     * Removes fields added by streamx-docker-hub-public-proxy wrapper for opensearch
     */
    private static function removeFieldsAddedByOpensearchWrapper(array &$jsonArray): void {
        unset($jsonArray['category']);
        unset($jsonArray['ingested']);
        foreach ($jsonArray as $key => $value) {
            if (strpos($key, 'ft_') === 0) {
                unset($jsonArray[$key]);
            }
        }
    }

    private static function normalizeArray(array &$jsonArray): void {
        if (array_keys($jsonArray) !== range(0, count($jsonArray) - 1)) {
            // sort associative arrays by key (don't sort items in indexed arrays)
            ksort($jsonArray);
        }

        foreach ($jsonArray as &$value) {
            if (is_array($value)) {
                self::normalizeArray($value);
            }
        }
    }
}