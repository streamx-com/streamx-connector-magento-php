<?php

namespace StreamX\ConnectorCatalog\test\integration;

use PHPUnit\Framework\TestCase;
use StreamX\ConnectorCatalog\Indexer\AttributeIndexer;
use StreamX\ConnectorCatalog\Indexer\CategoryIndexer;
use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoEndpointsCaller;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoLogFileUtils;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoMySqlQueryExecutor;

/**
 * @inheritDoc
 *  Note: see InitControllerImpl for additional stores and website created for these tests
 */
abstract class BaseStreamxConnectorPublishTest extends BaseStreamxTest {

    const UPDATE_ON_SAVE = 'update-on-save';
    const UPDATE_BY_SCHEDULE = 'update-by-schedule';

    // every test operates on one or more indexes which share the same mode (UPDATE_ON_SAVE or UPDATE_BY_SCHEDULE)
    const INDEXER_MODE = '';
    const INDEXER_IDS = [];

    public const DEFAULT_STORE_ID = 0;

    protected static int $website1Id = 1;
    protected static int $store1Id = 1;
    protected static int $store2Id;
    protected static int $website2Id;
    protected static int $website2StoreId;

    protected const STORE_1_CODE = 'default';
    protected const STORE_2_CODE = 'store_2';
    protected const WEBSITE_2_CODE = 'second_website';
    protected const WEBSITE_2_STORE_CODE = 'store_for_second_website';

    protected static bool $areTestsInitialized = false;
    private static array $initialIndexerModes;

    protected static MagentoMySqlQueryExecutor $db;
    protected MagentoLogFileUtils $logFileUtils;

    public static function setUpBeforeClass(): void {
        if (!self::$areTestsInitialized) {
            self::initializeTests();
            self::$areTestsInitialized = true;
        }
        TestCase::assertNotEmpty(static::INDEXER_MODE);
        TestCase::assertNotEmpty(static::INDEXER_IDS);
        self::setIndexerModeInMagento();
    }

    public static function initializeTests(): void {
        self::$db = new MagentoMySqlQueryExecutor();
        self::loadInitialIndexerModes();
        self::loadStoreAndWebsiteIds();

        if (self::$db->isEnterpriseMagento()) {
            self::disableGiftCardsCategory();
        }
        self::assignTaxClassIdToTestProduct();
    }

    private static function loadInitialIndexerModes(): void {
        foreach ([ProductIndexer::INDEXER_ID, CategoryIndexer::INDEXER_ID, AttributeIndexer::INDEXER_ID] as $indexerId) {
            self::$initialIndexerModes[$indexerId] = self::getIndexerMode($indexerId);
        }
    }

    private static function loadStoreAndWebsiteIds(): void {
        self::$store2Id = self::loadIdByCode('store', 'store_id', self::STORE_2_CODE);
        self::$website2Id = self::loadIdByCode('store_website', 'website_id', self::WEBSITE_2_CODE);
        self::$website2StoreId = self::loadIdByCode('store', 'store_id', self::WEBSITE_2_STORE_CODE);
    }

    private static function loadIdByCode(string $table, string $column, string $code): int {
        return self::$db->selectSingleValue("SELECT $column FROM $table WHERE code = '$code'");
    }

    private static function disableGiftCardsCategory(): void {
        // disable a category that exists only in enterprise magento, to allow having common validation files for both versions
        $categoryId = self::$db->getCategoryId('Gift Cards');
        $isActiveAttributeId = self::$db->getCategoryAttributeId('is_active');
        self::$db->insertIntCategoryAttribute($categoryId, $isActiveAttributeId, 0);
    }

    private static function assignTaxClassIdToTestProduct() {
        // product with ID=1 is used by many tests, but, unlike other products, it initially doesn't have a Tax Class assigned. Fix that:
        self::$db->insertIntProductAttribute(new EntityIds(1, 1), self::$db->getProductAttributeId('tax_class_id'), 2);
    }

    public static function tearDownAfterClass(): void {
        self::restoreIndexerModeInMagento();
    }

    private static function setIndexerModeInMagento(): void {
        foreach (static::INDEXER_IDS as $testedIndexerId) {
            if (static::INDEXER_MODE !== self::$initialIndexerModes[$testedIndexerId]) {
                self::setIndexerMode($testedIndexerId, static::INDEXER_MODE);
            }
        }
    }

    private static function restoreIndexerModeInMagento(): void {
        foreach (static::INDEXER_IDS as $testedIndexerId) {
            $initialIndexerMode = self::$initialIndexerModes[$testedIndexerId];
            if (static::INDEXER_MODE !== $initialIndexerMode) {
                self::setIndexerMode($testedIndexerId, $initialIndexerMode);
            }
        }
    }

    protected static function getIndexerMode(string $indexerId): string {
        return MagentoEndpointsCaller::call('indexer/mode/get', [
            'indexerId' => $indexerId
        ]);
    }

    protected static function setIndexerMode(string $indexerId, string $mode): void {
        MagentoEndpointsCaller::call('indexer/mode/set', [
            'indexerId' => $indexerId,
            'mode' => $mode
        ]);
    }

    public static function reindexMview(): void {
        foreach (static::INDEXER_IDS as $testedIndexerId) {
            MagentoEndpointsCaller::call('mview/reindex', [
                // note: assuming that every indexer in the indexer.xml file shares its ID with its view ID
                'indexerViewId' => $testedIndexerId
            ]);
        }
    }

    protected function setUp(): void {
        parent::setUp();
        $this->logFileUtils = new MagentoLogFileUtils();
        $this->logFileUtils->appendLine('Starting test ' . get_class($this) . '.' . $this->getName());
    }

    protected function tearDown(): void {
        $testName = get_class($this) . '.' . $this->getName();
        $this->logFileUtils->appendLine("Finished test $testName with result " . $this->getStatus());
        $ingestedKeys = $this->logFileUtils->getPublishedAndUnpublishedKeys();
        if ($ingestedKeys->empty()) {
            fwrite(STDOUT, "No keys were ingested during $testName:\n");
        } else {
            fwrite(STDOUT, "Keys ingested during $testName:\n");
            fwrite(STDOUT, $ingestedKeys->formatted() . PHP_EOL);
        }
        parent::tearDown();
    }

    public static function productKey(EntityIds $productId, string $storeCode = self::STORE_1_CODE): string {
        return self::productKeyFromEntityId($productId->getEntityId(), $storeCode);
    }
    public static function productKeyFromEntityId(int $productEntityId, string $storeCode = self::STORE_1_CODE): string {
        return self::expectedStreamxKey($productEntityId, 'product', $storeCode);
    }

    public static function categoryKey(EntityIds $categoryId, string $storeCode = self::STORE_1_CODE): string {
        return self::categoryKeyFromEntityId($categoryId->getEntityId(), $storeCode);
    }
    public static function categoryKeyFromEntityId(int $categoryEntityId, string $storeCode = self::STORE_1_CODE): string {
        return self::expectedStreamxKey($categoryEntityId, 'category', $storeCode);
    }

    private static function expectedStreamxKey(int $entityId, string $type, string $storeCode): string {
        return "{$storeCode}_$type:$entityId";
    }

}