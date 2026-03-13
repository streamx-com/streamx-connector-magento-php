<?php

namespace StreamX\ConnectorCatalog\test\integration\utils;

use Exception;

final class FileUtils {

    /**
     * Goes up from the current dir, until finding folder with the given name, and returns its absolute path
     */
    public static function findFolder(string $folder): string {
        $currentDir = __DIR__; // Start from the current directory

        while (true) {
            if (is_dir("$currentDir/$folder")) {
                return "$currentDir/$folder";
            }

            $parentDir = dirname($currentDir);

            if ($parentDir === $currentDir) { // root dir reached
                break;
            }

            $currentDir = $parentDir;
        }

        throw new Exception("$folder folder not found");
    }

    public static function readSourceFileContent(string $sourceFilePathRelativeToProjectRootDir): string {
        $projectRootDir = self::findFolder('streamx-connector-magento-php');
        $sourceFileAbsolutePath = "$projectRootDir/$sourceFilePathRelativeToProjectRootDir";
        return file_get_contents($sourceFileAbsolutePath);
    }

    public static function readPropertiesFile(string $absolutePath): array {
        $properties = [];

        $file = fopen($absolutePath, "r");
        while (($line = fgets($file)) !== false) {
            if (str_contains($line, "=")) {
                $keyAndValue = explode("=", $line);
                $properties[trim($keyAndValue[0])] = trim($keyAndValue[1]);
            }
        }
        fclose($file);

        return $properties;
    }
}