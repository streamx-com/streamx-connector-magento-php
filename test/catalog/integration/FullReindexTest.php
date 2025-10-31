<?php

namespace StreamX\ConnectorCatalog\test\integration;

use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationKeyPaths;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoEndpointsCaller;

class FullReindexTest extends BaseStreamxConnectorPublishTest {

    const INDEXER_MODE = parent::UPDATE_ON_SAVE;
    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    /** @test */
    public function shouldReindexAllProducts() {
        // given: we will test some selected products
        $simpleProductId = self::$db->getProductId('Joust Duffle Bag');
        $groupedProductId = self::$db->getProductId('Set of Sprite Yoga Straps');
        $variantProductId = self::$db->getProductId('Chaz Kangeroo Hoodie-XL-Orange');
        $configurableProductId = self::$db->getProductId('Chaz Kangeroo Hoodie'); // this product is also assigned to second website, see StoresControllerImpl

        // and
        $simpleProductKey = parent::productKey($simpleProductId);
        $groupedProductKey = parent::productKey($groupedProductId);
        $variantProductKey = parent::productKey($variantProductId);
        $configurableProductKey = parent::productKey($configurableProductId);
        $configurableProductKeyInSecondStore = parent::productKey($configurableProductId, self::STORE_2_CODE);
        $configurableProductKeyInSecondWebsite = parent::productKey($configurableProductId, self::WEBSITE_2_STORE_CODE);

        self::removeFromStreamX(
            $simpleProductKey, $groupedProductKey, $variantProductKey,
            $configurableProductKey, $configurableProductKeyInSecondStore, $configurableProductKeyInSecondWebsite
        );

        try {
            // when
            ConfigurationEditUtils::setConfigurationValue(ConfigurationKeyPaths::EXPORT_PRODUCTS_NOT_VISIBLE_INDIVIDUALLY, '1'); // needed to publish variants
            self::runProductsIndexer();

            // then
            $this->assertExactDataIsPublished($simpleProductKey, 'original-bag-product.json');
            $this->assertExactDataIsPublished($groupedProductKey, 'original-grouped-product.json');
            $this->assertExactDataIsPublished($variantProductKey, 'original-hoodie-xl-orange-product.json', [
                'Not Visible Individually' => 'Catalog, Search' // adjust actual json to validation file content
            ]);
            $this->assertExactDataIsPublished($configurableProductKey, 'original-hoodie-product.json');
            $this->assertExactDataIsPublished($configurableProductKeyInSecondStore, 'original-hoodie-product.json');
            $this->assertExactDataIsPublished($configurableProductKeyInSecondWebsite, 'original-hoodie-product-in-second-website.json', [], 15);
        } finally {
            ConfigurationEditUtils::restoreConfigurationValue(ConfigurationKeyPaths::EXPORT_PRODUCTS_NOT_VISIBLE_INDIVIDUALLY);
        }
    }

    private function runProductsIndexer(): void {
        MagentoEndpointsCaller::call('indexer/run', [
            'indexerId' => ProductIndexer::INDEXER_ID,
            'entityIds' => [] // trigger full reindexing by not passing any IDs
        ]);
    }
}