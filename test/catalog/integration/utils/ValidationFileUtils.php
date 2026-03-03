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
            self::adjustExpectedJson($expectedJson);
            self::adjustActualJson($actualJson, $regexReplacements);
            $this->assertEquals($expectedJson, $actualJson);
            return true;
        } catch (ExpectationFailedException $e) {
            if ($throwOnAssertionError) {
                throw $e;
            }
            return false;
        }
    }

    private function adjustExpectedJson(string &$json): void {
        self::normalizeJson($json);
    }

    private function adjustActualJson(string &$json, array $regexReplacements = []): void {
        self::replaceRegexes($json, $regexReplacements);
        self::standardizeNewlines($json);
        self::normalizeJson($json);
    }

    private function replaceRegexes(string &$json, array $regexReplacements): void {
        foreach ($regexReplacements as $regex => $replacement) {
            $json = preg_replace("|$regex|m", $replacement, $json);
        }
    }

    private function standardizeNewlines(string &$json): void {
        $json = str_replace('\r\n', '\n', $json);
    }

    private function normalizeJson(string &$json): void {
        $jsonArray = json_decode($json, true);
        self::normalizeArray($jsonArray);
        $json = json_encode($jsonArray, JSON_PRETTY_PRINT);
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