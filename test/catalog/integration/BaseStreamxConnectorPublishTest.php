<?php

namespace StreamX\ConnectorCatalog\test\integration;

use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use ReflectionClass;
use StreamX\ConnectorCatalog\Model\Indexer\AttributeProcessor;
use StreamX\ConnectorCatalog\Model\Indexer\CategoryProcessor;
use StreamX\ConnectorCatalog\Model\Indexer\ProductProcessor;
use StreamX\ConnectorCatalog\test\integration\AppEntityUpdateStreamxPublishTests\BaseAppEntityUpdateTest;
use StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests\BaseDirectDbEntityUpdateTest;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditTraits;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoIndexerOperationsExecutor;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoLogFileUtils;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoMySqlQueryExecutor;

/**
 * @inheritDoc
 *  Note: see StoresControllerImpl for additional stores and website created for these tests
 */
abstract class BaseStreamxConnectorPublishTest extends BaseStreamxTest {

    use ConfigurationEditTraits;

    private const MAGENTO_REST_API_BASE_URL = 'https://magento.test:444/rest/all/V1';

    protected const DEFAULT_WEBSITE_ID = 1;
    protected const DEFAULT_STORE_ID = 0;
    protected const STORE_1_ID = 1;
    protected static int $store2Id;
    protected static int $secondWebsiteId;
    protected static int $secondWebsiteStoreId;

    protected static bool $areTestsInitialized = false;

    protected static MagentoMySqlQueryExecutor $db;

    protected static MagentoIndexerOperationsExecutor $indexerOperations;
    private static string $originalIndexerMode;
    private static bool $indexModeNeedsRestoring;

    protected static string $testedIndexerName;
    private static string $testedIndexerMode;

    private MagentoLogFileUtils $logFileUtils;

    public static function setUpBeforeClass(): void {
        if (!self::$areTestsInitialized) {
            self::initializeTests();
            self::$areTestsInitialized = true;
        }
        self::loadDesiredIndexerSettings();
        self::setIndexerModeInMagento();
    }

    public static function initializeTests(): void {
        self::$db = new MagentoMySqlQueryExecutor();
        self::$indexerOperations = new MagentoIndexerOperationsExecutor();

        if ("true" === self::callMagentoPutEndpoint('stores/setup')) {
            self::$indexerOperations->flushCache();
        }

        self::$store2Id = self::$db->selectSingleValue("SELECT store_id FROM store WHERE code = 'store_2_view'");
        self::$secondWebsiteId = self::$db->selectSingleValue("SELECT website_id FROM store_website WHERE code = 'second_website'");
        self::$secondWebsiteStoreId = self::$db->selectSingleValue("SELECT store_id FROM store WHERE code = 'store_view_for_second_website'");

        if (self::$db->isEnterpriseMagento()) {
            self::disableGiftCardsCategory();
        }
    }

    private static function disableGiftCardsCategory(): void {
        // disable a category that exists only in enterprise magento, to allow having common validation files for both versions
        $categoryId = self::$db->getCategoryId('Gift Cards');
        $isActiveAttributeId = self::$db->getCategoryAttributeId('is_active');
        self::$db->insertIntCategoryAttribute($categoryId, $isActiveAttributeId, 0, 0);
    }

    public static function tearDownAfterClass(): void {
        self::restoreIndexerModeInMagento();
    }

    private static function setIndexerModeInMagento(): void {
        self::$indexerOperations->setIndexerName(self::$testedIndexerName);
        self::$originalIndexerMode = self::$indexerOperations->getIndexerMode();

        if (self::$testedIndexerMode !== self::$originalIndexerMode) {
            self::$indexerOperations->setIndexerMode(self::$testedIndexerMode);
            self::$indexModeNeedsRestoring = true;
        } else {
            self::$indexModeNeedsRestoring = false;
        }
    }

    private static function restoreIndexerModeInMagento(): void {
        if (self::$indexModeNeedsRestoring) {
            self::$indexerOperations->setIndexerMode(self::$originalIndexerMode);
        }
    }

    private static function loadDesiredIndexerSettings(): void {
        $cls = new ReflectionClass(static::class);
        self::$testedIndexerName = self::getTestedIndexerName($cls);
        self::$testedIndexerMode = self::getTestedIndexerMode($cls);
    }

    private static function getTestedIndexerName(ReflectionClass $cls): string {
        $docComment = $cls->getDocComment();
        if (str_contains($docComment, '@UsesProductIndexer')) {
            return ProductProcessor::INDEXER_ID;
        }
        if (str_contains($docComment, '@UsesCategoryIndexer')) {
            return CategoryProcessor::INDEXER_ID;
        }
        if (str_contains($docComment, '@UsesAttributeIndexer')) {
            return AttributeProcessor::INDEXER_ID;
        }
        throw new Exception("Cannot detect indexer to use for $cls");
    }

    private static function getTestedIndexerMode(ReflectionClass $cls): string {
        if ($cls->isSubclassOf(BaseAppEntityUpdateTest::class)) {
            return MagentoIndexerOperationsExecutor::UPDATE_ON_SAVE_DISPLAY_NAME;
        }
        if ($cls->isSubclassOf(BaseDirectDbEntityUpdateTest::class)) {
            // Magento creates triggers to save db-level changes only when the scheduler is in the below mode:
            return MagentoIndexerOperationsExecutor::UPDATE_BY_SCHEDULE_DISPLAY_NAME;
        }
        throw new Exception("Cannot detect desired indexer mode for $cls");
    }

    protected static function callMagentoPutEndpoint(string $relativeUrl, array $params = []): string {
        $endpointUrl = self::MAGENTO_REST_API_BASE_URL . "/$relativeUrl?XDEBUG_SESSION_START=PHPSTORM";
        $jsonBody = json_encode($params);
        $headers = ['Content-Type' => 'application/json; charset=UTF-8'];

        $request = new Request('PUT', $endpointUrl, $headers, $jsonBody);
        $httpClient = new Client(['verify' => false]);
        $response = $httpClient->sendRequest($request);
        $responseBody = (string)$response->getBody();
        if ($response->getStatusCode() !== 200) {
            throw new Exception('Unexpected status code: ' . $response->getStatusCode());
        }

        return $responseBody;
    }

    protected function setUp(): void {
        echo "Starting {$this->getName()}\n";
        $this->logFileUtils = new MagentoLogFileUtils();
    }

    protected function tearDown(): void {
        $ingestedKeys = $this->logFileUtils->getPublishedAndUnpublishedKeys();
        echo 'Keys ingested during the test:' . PHP_EOL;
        echo $ingestedKeys->formatted() . PHP_EOL;
    }
}