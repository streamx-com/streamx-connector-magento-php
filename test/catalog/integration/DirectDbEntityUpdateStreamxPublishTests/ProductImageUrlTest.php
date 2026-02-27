<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationKeyPaths;
use StreamX\ConnectorCatalog\test\integration\utils\StoreLevelConfigurationEditUtils;

/**
 * @inheritdoc
 */
class ProductImageUrlTest extends BaseDirectDbEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishProductWithCustomBaseImageUrls() {
        // given
        $productId = self::$db->getProductId('Joust Duffle Bag');

        // and
        $expectedKey = self::productKey($productId, parent::STORE_2_CODE);
        self::removeFromStreamX($expectedKey);

        // when
        self::setBaseLinkUrl(self::$store2Id, 'https://my-cdn.com');
        self::$db->productDummyUpdate($productId);

        // then
        try {
            self::reindexMview();
            $this->assertExactDataIsPublished($expectedKey, "original-bag-product.json", [
                'my-cdn.com' => 'magento-app-1:444'
            ]);
        } finally {
            self::$db->revertProductDummyUpdate($productId);
            self::unsetBaseLinkUrl(self::$store2Id);
        }
    }

    private static function setBaseLinkUrl(int $storeId, string $baseLinkUrl): void {
        StoreLevelConfigurationEditUtils::setConfigurationValue(
            ConfigurationKeyPaths::BASE_SECURE_LINK_URL,
            $storeId,
            $baseLinkUrl
        );
    }

    private static function unsetBaseLinkUrl(int $storeId): void {
        StoreLevelConfigurationEditUtils::removeConfigurationValue(
            ConfigurationKeyPaths::BASE_SECURE_LINK_URL,
            $storeId
        );
    }
}