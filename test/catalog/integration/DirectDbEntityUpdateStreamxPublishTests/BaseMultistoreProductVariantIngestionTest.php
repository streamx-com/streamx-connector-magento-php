<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use Magento\Catalog\Model\Product\Attribute\Source\Status;
use Magento\Catalog\Model\Product\Visibility;
use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;

/**
 * @inheritdoc
 */
abstract class BaseMultistoreProductVariantIngestionTest extends BaseDirectDbEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    private const PARENT_ID = 62;
    private const VARIANT_1_ID = 60;
    private const VARIANT_2_ID = 61;

    private const PARENT_NAME = 'Chaz Kangeroo Hoodie';
    private const VARIANT_1_NAME = 'Chaz Kangeroo Hoodie-XL-Gray';
    private const VARIANT_2_NAME = 'Chaz Kangeroo Hoodie-XL-Orange';

    private const PARENT_JSON_FILE_WITH_ALL_VARIANTS = 'original-hoodie-product.json';
    private const VARIANT_1_JSON_FILE = 'original-hoodie-xl-gray-product.json';
    private const VARIANT_2_JSON_FILE = 'original-hoodie-xl-orange-product.json';
    private const PARENT_JSON_FILE_WITHOUT_VARIANT_2 = 'original-hoodie-product-without-xl-orange-variant.json';

    protected static EntityIds $parent;
    protected static EntityIds $variant1;
    protected static EntityIds $variant2;

    protected static string $keyOfParentInStore1;
    protected static string $keyOfVariant1InStore1;
    protected static string $keyOfVariant2InStore1;

    protected static string $keyOfParentInStore2;
    protected static string $keyOfVariant1InStore2;
    protected static string $keyOfVariant2InStore2;

    private static array $variantJsonByKeyMap;

    private static int $statusAttributeId;
    private static int $visibilityAttributeId;

    public static function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        self::$parent = self::$db->getProductId(self::PARENT_NAME);
        self::$variant1 = self::$db->getProductId(self::VARIANT_1_NAME);
        self::$variant2 = self::$db->getProductId(self::VARIANT_2_NAME);

        self::$keyOfParentInStore1 = self::productKey(self::$parent, self::STORE_1_CODE);
        self::$keyOfVariant1InStore1 = self::productKey(self::$variant1, self::STORE_1_CODE);
        self::$keyOfVariant2InStore1 = self::productKey(self::$variant2, self::STORE_1_CODE);

        self::$keyOfParentInStore2 = self::productKey(self::$parent, self::STORE_2_CODE);
        self::$keyOfVariant1InStore2 = self::productKey(self::$variant1, self::STORE_2_CODE);
        self::$keyOfVariant2InStore2 = self::productKey(self::$variant2, self::STORE_2_CODE);

        self::$variantJsonByKeyMap = [
            self::$keyOfVariant1InStore1 => self::VARIANT_1_JSON_FILE,
            self::$keyOfVariant1InStore2 => self::VARIANT_1_JSON_FILE,
            self::$keyOfVariant2InStore1 => self::VARIANT_2_JSON_FILE,
            self::$keyOfVariant2InStore2 => self::VARIANT_2_JSON_FILE
        ];

        self::$visibilityAttributeId = self::$db->getProductAttributeId('visibility');
        self::$statusAttributeId = self::$db->getProductAttributeId('status');
    }

    protected function setUp(): void {
        parent::setUp();

        // prepare initial state for every test in this class: make all tested products published (so every test can verify if some are unpublished)
        $this->removeFromStreamX(
            self::$keyOfParentInStore1, self::$keyOfVariant1InStore1, self::$keyOfVariant2InStore1,
            self::$keyOfParentInStore2, self::$keyOfVariant1InStore2, self::$keyOfVariant2InStore2
        );

        $streamxClient = parent::createStreamxClient();

        $publishProductsPayload = [
            self::PARENT_ID => $this->readJsonFileToArray(self::PARENT_JSON_FILE_WITH_ALL_VARIANTS),
            self::VARIANT_1_ID => $this->readJsonFileToArray(self::VARIANT_1_JSON_FILE),
            self::VARIANT_2_ID => $this->readJsonFileToArray(self::VARIANT_2_JSON_FILE)
        ];

        $store1 = parent::createStoreMock(self::$store1Id, self::STORE_1_CODE);
        $store2 = parent::createStoreMock(self::$store2Id, self::STORE_2_CODE);
        $streamxClient->publish($publishProductsPayload, ProductIndexer::INDEXER_ID, $store1);
        $streamxClient->publish($publishProductsPayload, ProductIndexer::INDEXER_ID, $store2);

        // wait until all three products are published from both stores
        $this->assertParentIsPublishedWithAllVariantsInPayload(self::$keyOfParentInStore1);
        $this->assertParentIsPublishedWithAllVariantsInPayload(self::$keyOfParentInStore2);

        $this->assertVariantsArePublished(
            self::$keyOfVariant1InStore1,
            self::$keyOfVariant1InStore2,
            self::$keyOfVariant2InStore1,
            self::$keyOfVariant2InStore2
        );

        parent::setUp();
    }

    private function readJsonFileToArray(string $path): array {
        return json_decode($this->readValidationFileContent($path), true);
    }

    protected function tearDown(): void {
        parent::tearDown();

        // restore initial state of DB
        foreach ([self::$parent, self::$variant1, self::$variant2] as $product) {
            self::$db->deleteIntProductAttribute($product, self::$statusAttributeId, self::$store2Id);
            self::$db->deleteIntProductAttribute($product, self::$visibilityAttributeId, self::$store2Id);
        }

        // flush all changes
        $this->reindexMview();

        // clean up state on StreamX
        self::removeFromStreamX(
            self::$keyOfParentInStore1, self::$keyOfVariant1InStore1, self::$keyOfVariant2InStore1,
            self::$keyOfParentInStore2, self::$keyOfVariant1InStore2, self::$keyOfVariant2InStore2
        );
    }

    protected function disableProductInStore2(EntityIds $product): void {
        self::$db->insertIntProductAttribute($product, self::$statusAttributeId, Status::STATUS_DISABLED, self::$store2Id);
    }
    protected function enableProductInStore2(EntityIds $product): void {
        self::$db->deleteIntProductAttribute($product, self::$statusAttributeId, self::$store2Id);
    }

    protected function makeParentProductInvisibleInStore2(): void {
        self::$db->insertIntProductAttribute(self::$parent, self::$visibilityAttributeId, Visibility::VISIBILITY_NOT_VISIBLE, self::$store2Id);
    }
    protected function makeParentProductVisibleInStore2(): void {
        self::$db->deleteIntProductAttribute(self::$parent, self::$visibilityAttributeId, self::$store2Id);
    }

    protected function assertParentIsPublishedWithAllVariantsInPayload(string $key): void {
        $this->assertDataIsPublished($key, self::PARENT_JSON_FILE_WITH_ALL_VARIANTS);
    }

    protected function assertParentIsPublishedWithoutVariant2InPayload(string $key): void {
        $this->assertDataIsPublished($key, self::PARENT_JSON_FILE_WITHOUT_VARIANT_2);
    }

    protected function assertParentIsNotPublished(string $key): void {
        $this->assertDataIsNotPublished($key);
    }

    protected function assertVariantIsPublished(string $key): void {
        $jsonFile = self::$variantJsonByKeyMap[$key];
        $this->assertDataIsPublished($key, $jsonFile);
    }

    protected function assertVariantsArePublished(string... $keys): void {
        foreach ($keys as $key) {
            $this->assertVariantIsPublished($key);
        }
    }

    protected function assertVariantIsNotPublished(string $key): void {
        $this->assertDataIsNotPublished($key);
    }

    private function assertDataIsPublished(string $key, string $validationFileName) {
        // the tests switch visibility of products, so normalize this here
        $this->assertExactDataIsPublished($key, $validationFileName, [
            'Not Visible Individually' => 'Catalog, Search'
        ]);
    }
}