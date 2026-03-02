<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\Model\Config\Source\SlugOptionsSource;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationKeyPaths;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;

/**
 * @inheritdoc
 */
class ProductPublishSlugTest extends BaseDirectDbEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    private const DEFAULT_PRODUCT_JSON = 'original-bag-product.json';
    private const DEFAULT_PRODUCT_SLUG = 'joust-duffle-bag-1';
    private const DEFAULT_GEAR_CATEGORY_SLUG = 'gear-3';
    private const DEFAULT_BAGS_CATEGORY_SLUG = 'bags-4';

    private const EDITED_PRODUCT_URL_KEY = 'edited-product-url-key';
    private const EDITED_GEAR_CATEGORY_URL_KEY = 'edited-gear-url-key';
    private const EDITED_BAGS_CATEGORY_URL_KEY = 'edited-bags-url-key';

    private EntityIds $productId;
    private EntityIds $gearCategoryId;
    private EntityIds $bagsCategoryId;

    private int $productUrlKeyAttributeId;
    private int $categoryUrlKeyAttributeId;

    private string $productKey;

    protected function setUp(): void {
        parent::setUp();
        $this->productId = self::$db->getProductId('Joust Duffle Bag');
        $this->gearCategoryId = self::$db->getCategoryId('Gear');
        $this->bagsCategoryId = self::$db->getCategoryId('Bags');
        $this->productUrlKeyAttributeId = self::$db->getProductAttributeId('url_key');
        $this->categoryUrlKeyAttributeId = self::$db->getCategoryAttributeId('url_key');

        $this->productKey = self::productKey($this->productId, self::STORE_2_CODE);
        self::removeFromStreamX($this->productKey);
    }

    /** @test */
    public function shouldPublishProductWithSlug_FromNameAndId() {
        $this->shouldPublishProductEditedUsingMagentoApplication(
            SlugOptionsSource::NAME_AND_ID,
            self::DEFAULT_PRODUCT_SLUG,
            self::DEFAULT_GEAR_CATEGORY_SLUG,
            self::DEFAULT_BAGS_CATEGORY_SLUG
        );
    }

    /** @test */
    public function shouldPublishProductWithSlug_FromUrlKey() {
        $this->shouldPublishProductEditedUsingMagentoApplication(
            SlugOptionsSource::URL_KEY,
            'edited-product-url-key',
            'edited-gear-url-key',
            'edited-bags-url-key'
        );
    }

    /** @test */
    public function shouldPublishProductWithSlug_FromUrlKeyAndId() {
        $this->shouldPublishProductEditedUsingMagentoApplication(
            SlugOptionsSource::URL_KEY_AND_ID,
            'edited-product-url-key-1',
            'edited-gear-url-key-3',
            'edited-bags-url-key-4'
        );
    }

    private function shouldPublishProductEditedUsingMagentoApplication(int $slugGenerationStrategy,
                                                                       string $expectedProductSlug, string $expectedGearCategorySlug, string $expectedBagsCategorySlug): void {
        // given: change all url keys, to make sure they are different from names
        $this->changeProductUrlKey($this->productId, self::EDITED_PRODUCT_URL_KEY);
        $this->changeCategoryUrlKey($this->gearCategoryId, self::EDITED_GEAR_CATEGORY_URL_KEY);
        $this->changeCategoryUrlKey($this->bagsCategoryId, self::EDITED_BAGS_CATEGORY_URL_KEY);

        // and:
        $expectedRegexReplacements = [
            // expect slugs different from those in validation file
            '"slug":"' . $expectedProductSlug . '"' => '"slug":"' . self::DEFAULT_PRODUCT_SLUG . '"',
            '"slug":"' . $expectedGearCategorySlug . '"' => '"slug":"' . self::DEFAULT_GEAR_CATEGORY_SLUG . '"',
            '"slug":"' . $expectedBagsCategorySlug . '"' => '"slug":"' . self::DEFAULT_BAGS_CATEGORY_SLUG . '"',
            // the test also edits url key attribute of the product
            '"edited-product-url-key"' => '"joust-duffle-bag"'
        ];

        // when
        ConfigurationEditUtils::setConfigurationValue(ConfigurationKeyPaths::SLUG_GENERATION_STRATEGY, $slugGenerationStrategy);
        parent::reindexMview();

        try {
            // then
            $this->assertExactDataIsPublished($this->productKey, self::DEFAULT_PRODUCT_JSON, $expectedRegexReplacements);
        } finally {
            // restore changes
            ConfigurationEditUtils::restoreConfigurationValue(ConfigurationKeyPaths::SLUG_GENERATION_STRATEGY);
            $this->restoreProductUrlKey($this->productId);
            $this->restoreCategoryUrlKey($this->gearCategoryId);
            $this->restoreCategoryUrlKey($this->bagsCategoryId);
        }
    }

    private function changeProductUrlKey(EntityIds $productId, string $newUrlKey): void {
        self::$db->insertVarcharProductAttribute($productId, $this->productUrlKeyAttributeId, $newUrlKey, parent::$store2Id);
    }

    private function restoreProductUrlKey(EntityIds $productId): void {
        self::$db->deleteVarcharProductAttribute($productId, $this->productUrlKeyAttributeId, parent::$store2Id);
    }

    private function changeCategoryUrlKey(EntityIds $categoryId, string $newUrlKey): void {
        self::$db->insertVarcharCategoryAttribute($categoryId, $this->categoryUrlKeyAttributeId, $newUrlKey, parent::$store2Id);
    }

    private function restoreCategoryUrlKey(EntityIds $categoryId): void {
        self::$db->deleteVarcharCategoryAttribute($categoryId, $this->categoryUrlKeyAttributeId, parent::$store2Id);
    }
}