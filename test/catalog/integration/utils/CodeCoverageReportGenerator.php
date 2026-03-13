<?php

namespace StreamX\ConnectorCatalog\test\integration\utils;

use PHPUnit\Framework\TestCase;
use PHPUnit\Runner\BaseTestRunner;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SebastianBergmann\CodeCoverage\CodeCoverage;
use SebastianBergmann\CodeCoverage\Filter;
use SebastianBergmann\CodeCoverage\ProcessedCodeCoverageData;
use SebastianBergmann\CodeCoverage\RawCodeCoverageData;
use SebastianBergmann\CodeCoverage\Report\Html\Facade;
use SebastianBergmann\CodeCoverage\StaticAnalysis\ParsingFileAnalyser;
use const true;

final class CodeCoverageReportGenerator {

    private const STREAMX_CONNECTOR_ROOT_DIR_IN_MAGENTO_SERVER = '/var/www/html/app/code/StreamX/Connector';

    public static function isCoverageMeasurementEnabledOnMagentoServer(): bool {
        $magentoFolder = FileUtils::findFolder('magento');
        $properties = FileUtils::readPropertiesFile("$magentoFolder/env/phpfpm.env");
        return array_key_exists('XDEBUG_MODE', $properties) && $properties['XDEBUG_MODE'] === 'coverage';
    }

    public static function generateSingleTestCodeCoverageReport(TestCase $caller): void {
        self::generateCodeCoverageReport($caller, false);
    }

    public static function generateSummaryCodeCoverageReport(TestCase $caller): void {
        self::generateCodeCoverageReport($caller, true);
    }

    private static function generateCodeCoverageReport(TestCase $caller, bool $includeCoverageDataFromPreviousTests): void {
        if (!self::isCoverageMeasurementEnabledOnMagentoServer()) {
            return;
        }

        $coverageFiles = self::getCoverageFiles($includeCoverageDataFromPreviousTests);
        if (empty($coverageFiles)) {
            return;
        }
        $summaryCoverageData = CodeCoverageDataMerger::merge($coverageFiles);

        $localConnectorRootDir = self::getLocalConnectorRootDir();
        $coveredData = self::removeNonStreamxConnectorEntries($summaryCoverageData, $localConnectorRootDir);

        $testName = $caller->getName();
        self::adjustCoverageData($coveredData, $testName);
        self::addMissingFilesAsUncovered($coveredData, $localConnectorRootDir);

        $uncoveredData = self::computeUncoveredData($coveredData);
        $codeCoverage = self::createCodeCoverageObject($testName, $coveredData, $uncoveredData);

        self::writeAsHtmlReport($localConnectorRootDir, $caller, $codeCoverage);
    }

    private static function adjustCoverageData(array &$coverageArray, string $testName): void {
        foreach ($coverageArray as $file => &$coverageData) {
            foreach ($coverageData as $lineNumber => &$value) {
                if ($value === 1) {
                    $value = [0 => $testName];
                }
            }
        }
    }

    private static function removeNonStreamxConnectorEntries(array $coverageData, string $localConnectorRootDir): array {
        $filteredCoverageData = [];
        foreach ($coverageData as $filePath => $value) {
            $unescapedFilePath = str_replace('\\/', '/', $filePath);
            if (str_starts_with($unescapedFilePath, self::STREAMX_CONNECTOR_ROOT_DIR_IN_MAGENTO_SERVER) && !str_contains($unescapedFilePath, 'ConnectorTestEndpoints')) {
                $localFilePath = str_replace(
                    self::STREAMX_CONNECTOR_ROOT_DIR_IN_MAGENTO_SERVER,
                    $localConnectorRootDir,
                    $unescapedFilePath
                );
                $filteredCoverageData[$localFilePath] = $value;
            }
        }
        return $filteredCoverageData;
    }

    // xdebug doesn't return any coverage info about files that were not executed at all
    private static function addMissingFilesAsUncovered(array &$coverageArray, string $localConnectorRootDir): void {
        $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator("$localConnectorRootDir/src"));
        foreach ($iterator as $path) {
            if ($path->isFile() && $path->getExtension() === 'php') {
                $fullPath = $path->getRealPath();
                if (!array_key_exists($fullPath, $coverageArray)) {
                    $coverageArray[$fullPath] = [];
                }
            }
        }
    }

    private static function computeUncoveredData(array $coverageArray): ProcessedCodeCoverageData {
        $uncoveredData = new ProcessedCodeCoverageData();
        $analyser = new ParsingFileAnalyser(false, false);
        foreach (array_keys($coverageArray) as $filePath) {
            $rawCodeCoverageData = RawCodeCoverageData::fromUncoveredFile($filePath, $analyser);
            $uncoveredData->initializeUnseenData($rawCodeCoverageData);
        }
        return $uncoveredData;
    }

    private static function createCodeCoverageObject(string $testName, array $coveredData, ProcessedCodeCoverageData $uncoveredData): CodeCoverage {
        $codeCoverage = new CodeCoverage(new CodeCoverageDriverMock(), new Filter());

        $codeCoverage->setTests([
            $testName => [
                'size' => 'integration',
                'fromTestcase' => true,
                'status' => BaseTestRunner::STATUS_PASSED
            ]
        ]);

        $mergedCoverageData = new ProcessedCodeCoverageData();
        $mergedCoverageData->setLineCoverage($coveredData);
        $mergedCoverageData->merge($uncoveredData);

        $codeCoverage->setData($mergedCoverageData);
        return $codeCoverage;
    }

    private static function writeAsHtmlReport(string $localConnectorRootDir, TestCase $caller, CodeCoverage $codeCoverage): void {
        $htmlReportFacade = new Facade();
        $reportDirectory = self::createCoverageReportDirectory($localConnectorRootDir, $caller);
        $htmlReportFacade->process($codeCoverage, $reportDirectory);
    }

    private static function createCoverageReportDirectory(string $localConnectorRootDir, TestCase $caller): string {
        $className = str_replace('\\', '/', get_class($caller));
        $testName = $caller->getName();
        $dir = "$localConnectorRootDir/target/coverage-reports/$className/$testName";
        if (is_dir($dir)) {
            system("rm -rf " . escapeshellarg($dir));
        }
        mkdir($dir, 0777, true);
        echo "Code coverage report created: $dir/index.html\n";
        return $dir;
    }

    private static function getCoverageFiles(bool $includeCoverageDataFromPreviousTests): array {
        $localConnectorRootDir = self::getLocalConnectorRootDir();
        $coverageFilesDirectory = self::getCoverageFilesDirectory($localConnectorRootDir);
        $result = [];
        if (is_dir($coverageFilesDirectory)) {
            if ($includeCoverageDataFromPreviousTests) {
                $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($coverageFilesDirectory));
                foreach ($iterator as $path) {
                    if ($path->isFile()) {
                        $result[] = $path->getRealPath();
                    }
                }
            } else {
                foreach (scandir($coverageFilesDirectory) as $file) {
                    $filePath = "$coverageFilesDirectory/$file";
                    if (is_file($filePath)) {
                        $result[] = $filePath;
                    }
                }
            }
        }
        return $result;
    }

    public static function hideCoverageFilesFromPreviousTest(): void {
        if (!self::isCoverageMeasurementEnabledOnMagentoServer()) {
            return;
        }

        $localConnectorRootDir = self::getLocalConnectorRootDir();
        $coverageFilesDirectory = self::getCoverageFilesDirectory($localConnectorRootDir);
        $previousFilesDirectory = "$coverageFilesDirectory/previous";
        if (!is_dir($previousFilesDirectory)) {
            mkdir($previousFilesDirectory, 0777, true);
        }
        foreach (self::getCoverageFiles(false) as $coverageFile) {
            rename($coverageFile, $previousFilesDirectory . '/' . basename($coverageFile));
        }
    }

    private static function getLocalConnectorRootDir(): string {
        return FileUtils::findFolder('streamx-connector-magento-php');
    }

    private static function getCoverageFilesDirectory(string $localConnectorRootDir): string {
        return "$localConnectorRootDir/magento/src/app/code/StreamX/ConnectorTestEndpoints/coverage";
    }
}