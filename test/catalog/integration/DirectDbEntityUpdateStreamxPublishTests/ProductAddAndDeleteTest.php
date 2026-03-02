<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use DateTime;
use Magento\Catalog\Model\Product\Attribute\Source\Status;
use Magento\Catalog\Model\Product\Visibility;
use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;

/**
 * @inheritdoc
 */
class ProductAddAndDeleteTest extends BaseDirectDbEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    private const PRODUCT_PRICE = 350;
    private const INDEXED_PRICE = 370;
    private const DISCOUNTED_PRICE = 330;

    /** @test */
    public function shouldPublishMinimalProductAddedDirectlyInDatabase_AndUnpublishDeletedProduct() {
        // given
        $productName = 'The minimal product';
        $sku = (string) (new DateTime())->getTimestamp();

        // when
        $product = $this->insertNewMinimalProduct($sku, $productName);
        $expectedKey = self::productKey($product);

        try {
            // and
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished($expectedKey, 'added-minimal-product.json', [
                // provide values for placeholders in the validation file
                $sku => 'SKU',
                '"id":"' . $product->getEntityId() . '"' => '"id":"123456789"',
                'The minimal product' => 'PRODUCT_NAME',
                "the-minimal-product-{$product->getEntityId()}" => 'PRODUCT_SLUG',
                'Catalog, Search' => 'VISIBILITY'
            ]);
        } finally {
            // and when
            $this->deleteProduct($product);
            $this->reindexMview();

            // then
            $this->assertDataIsUnpublished($expectedKey);
        }
    }

    /** @test */
    public function shouldPublishProductAddedDirectlyInDatabase_AndUnpublishDeletedProduct() {
        // given
        $productName = 'The new great watch';
        $categoryIds = [
            self::$db->getCategoryId('Watches'),
            self::$db->getCategoryId('Collections'), // note: this category is not active in sample data by default
            self::$db->getCategoryId('Sale')
        ];

        // when
        ConfigurationEditUtils::allowIndexingAllProductAttributes();
        $product = $this->insertNewProduct($productName, $categoryIds);
        $expectedKey = self::productKey($product);

        try {
            // and
            $this->reindexMview();

            // then
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
                $this->deleteProduct($product);
                $this->reindexMview();

                // then
                $this->assertDataIsUnpublished($expectedKey);
            } finally {
                ConfigurationEditUtils::restoreDefaultIndexedProductAttributes();
            }
        }
    }

    /** @test */
    public function shouldNotPublishNotActiveProduct() {
        // given
        $productName = 'The second watch';
        $watchesCategoryId = self::$db->getCategoryId('Watches');

        // when
        $product = $this->insertNewProduct($productName, [$watchesCategoryId]);
        $expectedKey = self::productKey($product);

        // and: make the product not active in default store:
        self::$db->insertIntProductAttribute($product, self::attrId('status'), Status::STATUS_DISABLED, self::$store1Id);

        try {
            // and
            $this->reindexMview();

            // then
            $this->assertDataIsNotPublished($expectedKey);
        } finally {
            $this->deleteProduct($product);
        }
    }

    /** @test */
    public function shouldPublishMultipleProductsAddedDirectlyInDatabase_AndUnpublishDeletedProducts() {
        // given
        $productsCount = 200;

        // when
        $productNames = [];
        $skus = [];
        $productsIds = [];
        for ($i = 0; $i < $productsCount; $i++) {
            $productNames[] = "Watch $i";
            $skus[] = (string) (new DateTime())->getTimestamp();
            $productsIds[] = $this->insertNewMinimalProduct($skus[$i], $productNames[$i]);
        }

        try {
            // and
            $this->reindexMview();

            // then
            for ($i = 0; $i < $productsCount; $i++) {
                $productId = $productsIds[$i]->getEntityId();
                $productName = $productNames[$i];
                $expectedSlug = str_replace([' ', 'W'], ['-', 'w'], $productName) . "-$productId";
                $this->assertExactDataIsPublished("default_product:$productId", 'added-minimal-product.json', [
                    // provide values for placeholders in the validation file
                    $skus[$i] => 'SKU',
                    '"id":"' . $productId . '"' => '"id":"123456789"',
                    $productName => 'PRODUCT_NAME',
                    $expectedSlug => 'PRODUCT_SLUG',
                    'Catalog, Search' => 'VISIBILITY'
                ]);
            }
        } finally {
            // and when
            for ($i = 0; $i < $productsCount; $i++) {
                $this->deleteProduct($productsIds[$i]);
            }
            $this->reindexMview();

            // then
            for ($i = 0; $i < $productsCount; $i++) {
                $this->assertDataIsUnpublished(self::productKey($productsIds[$i]));
            }
        }
    }

    private function insertNewMinimalProduct(string $sku, string $productName): EntityIds {
        $product = self::$db->insertProduct($sku, self::$website1Id);

        self::$db->insertVarcharProductAttribute($product, self::attrId('name'), $productName);
        self::$db->insertIntProductAttribute($product, self::attrId('status'), Status::STATUS_ENABLED);
        self::$db->insertIntProductAttribute($product, self::attrId('visibility'), Visibility::VISIBILITY_BOTH);

        return $product;
    }

    private function insertNewProduct(string $productName, array $categoryIds): EntityIds {
        $sku = (string) (new DateTime())->getTimestamp();
        $productInternalName = strtolower(str_replace(' ', '-', $productName));

        $stockId = 1;
        $quantity = 100;
        $websiteId = self::$website1Id;
        $brownColorId = self::$db->getAttributeOptionId('color', 'Brown');
        $metalMaterialId = self::$db->getAttributeOptionId('material', 'Metal');
        $plasticMaterialId = self::$db->getAttributeOptionId('material', 'Plastic');
        $leatherMaterialId = self::$db->getAttributeOptionId('material', 'Leather');

        $product = self::$db->insertProduct($sku, $websiteId);
        $productId = $product->getEntityId();

        self::$db->insertVarcharProductAttribute($product, self::attrId('name'), $productName);
        self::$db->insertVarcharProductAttribute($product, self::attrId('meta_title'), $productName);
        self::$db->insertVarcharProductAttribute($product, self::attrId('meta_description'), $productName);
        self::$db->insertVarcharProductAttribute($product, self::attrId('url_key'), $productInternalName);
        self::$db->insertTextProductAttribute($product, self::attrId('material'), "$metalMaterialId,$plasticMaterialId,$leatherMaterialId");
        self::$db->insertDecimalProductAttribute($product, self::attrId('price'), self::PRODUCT_PRICE);
        self::$db->insertIntProductAttribute($product, self::attrId('visibility'), Visibility::VISIBILITY_BOTH);
        self::$db->insertIntProductAttribute($product, self::attrId('status'), Status::STATUS_ENABLED);
        self::$db->insertIntProductAttribute($product, self::attrId('color'), $brownColorId);

        foreach ($categoryIds as $categoryId) {
            self::$db->execute("
                INSERT INTO catalog_category_product (category_id, product_id, position) VALUES
                    ({$categoryId->getEntityId()}, $productId, 0)
            ");
        }

        self::$db->executeQueries("
            INSERT INTO cataloginventory_stock_item (product_id, stock_id, qty, is_in_stock, is_qty_decimal, manage_stock) VALUES
                ($productId, $stockId, $quantity, TRUE, FALSE, TRUE)
        ", "
            INSERT INTO cataloginventory_stock_status (product_id, website_id, stock_id, qty, stock_status) VALUES
                ($productId, $websiteId, $stockId, $quantity, 1)
        ", "
            INSERT INTO catalog_product_index_price (entity_id, customer_group_id, website_id, price, final_price) VALUES
                ($productId, 0, $websiteId, " . self::INDEXED_PRICE . ", " . self::DISCOUNTED_PRICE . ")
        ");

        return $product;
    }

    private function deleteProduct(EntityIds $productIds): void {
        self::$db->deleteById($productIds->getEntityId(), [
            'catalog_product_index_price' => 'entity_id',
            'cataloginventory_stock_status' => 'product_id',
            'cataloginventory_stock_item' => 'product_id',
            'catalog_category_product' => 'product_id',
            'catalog_product_website' => 'product_id',
            'catalog_product_entity' => 'entity_id'
        ]);

        self::$db->deleteById($productIds->getLinkFieldId(), [
            'catalog_product_entity_decimal' => self::$db->getEntityAttributeLinkField(),
            'catalog_product_entity_int' => self::$db->getEntityAttributeLinkField(),
            'catalog_product_entity_varchar' => self::$db->getEntityAttributeLinkField(),
            'catalog_product_entity_text' => self::$db->getEntityAttributeLinkField()
        ]);

        if (self::$db->isEnterpriseMagento()) {
            self::$db->deleteById($productIds->getEntityId(), [
                'sequence_product' => 'sequence_value'
            ]);
        }
    }

    private static function attrId($attrCode): string {
        return self::$db->getProductAttributeId($attrCode);
    }
}