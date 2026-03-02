<?php

namespace StreamX\ConnectorCatalog\test\integration\AppEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoEndpointsCaller;

/**
 * @inheritdoc
 */
class ProductAddAndDeleteTest extends BaseAppEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishProductAddedUsingMagentoApplication_AndUnpublishDeletedProduct() {
        // given
        $productName = 'The new great watch';
        $categoryIds = [
            self::$db->getCategoryId('Watches'),
            self::$db->getCategoryId('Collections'), // note: this category is not active in sample data by default
            self::$db->getCategoryId('Sale')
        ];

        // when
        ConfigurationEditUtils::allowIndexingAllProductAttributes();
        $productId = self::addProduct($productName, $categoryIds);

        // then
        $expectedKey = self::productKeyFromEntityId($productId);
        try {
            $publishedJson = $this->assertExactDataIsPublished($expectedKey, 'added-watch-product.json', [
                // mask variable parts (ids and generated sku)
                '"id":"[0-9]{4,5}"' => '"id":"2659"',
                '"sku":"[^"]+"' => '"sku":"1736952738"',
                '"the-new-great-watch-[0-9]+"' => '"the-new-great-watch-2659"'
            ]);

            // and
            $this->assertStringContainsString('Watches', $publishedJson);
            $this->assertStringNotContainsString('Collections', $publishedJson);
            $this->assertStringContainsString('Sale', $publishedJson);
        } finally {
            try {
                // and when
                self::deleteProduct($productId);

                // then
                $this->assertDataIsUnpublished($expectedKey);
            } finally {
                ConfigurationEditUtils::restoreDefaultIndexedProductAttributes();
            }
        }
    }

    private function addProduct(string $productName, array $categories): int {
        $categoryIds = array_map(function (EntityIds $category) {
            return $category->getEntityId();
        }, $categories);

        return (int) MagentoEndpointsCaller::call('product/add', [
            'productName' => $productName,
            'categoryIds' => $categoryIds
        ]);
    }

    private function deleteProduct(int $productId): void {
        MagentoEndpointsCaller::call('product/delete', [
            'productId' => $productId
        ]);
    }
}