<?php declare(strict_types = 1);

namespace StreamX\ConnectorCatalog\test\integration\utils;

/**
 * Allows retrieving published and unpublished StreamX keys.
 * The time window is: between instantiating the object, and calling its getPublishedAndUnpublishedKeys method.
 */
class MagentoLogFileUtils  {

    private string $logFilePath;
    private int $logFileSize;

    public function __construct() {
        $magentoLogFilesDir = FileUtils::findFolder('magento/src/var/log');
        $this->logFilePath = "$magentoLogFilesDir/system.log";
        $this->logFileSize = filesize($this->logFilePath);
    }

    public function getPublishedAndUnpublishedKeys(): IngestedKeys {
        $newLogLines = $this->readNewLogFileLines();
        $result = new IngestedKeys();
        foreach ($newLogLines as $line) {
            if (str_contains($line, 'keys')) {
                $this->parseKeys($line, $result);
            }
        }
        return $result;
    }

    private function readNewLogFileLines(): array {
        $file = fopen($this->logFilePath, 'r');
        fseek($file, $this->logFileSize);

        $newLines = [];
        while ($newLine = fgets($file)) {
            $newLines[] = $newLine;
        }
        fclose($file);

        return $newLines;
    }

    private function parseKeys(string $line, IngestedKeys $result): void {
        $keysStartIndex = strpos($line, 'keys [') + 6;
        $keysEndIndex = strpos($line, '] [] []');
        $keys = explode(',', substr($line, $keysStartIndex, $keysEndIndex - $keysStartIndex));
        if (str_contains($line, 'unpublish')) {
            $result->addUnpublishedKeys($keys);
        } else if (str_contains($line, 'publish')) {
            $result->addPublishedKeys($keys);
        }
    }
}