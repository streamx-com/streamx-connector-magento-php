<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use DateTime;
use Magento\Catalog\Model\Product\Attribute\Source\Status;
use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;
use Magento\Catalog\Model\Product\Visibility;

/**
 * @inheritdoc
 */
class MultistoreProductPublishTest extends BaseDirectDbEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishProductsFromWebsite() {
        // given (as in InitControllerImpl), the following products exist in both websites:
        //  - simple products 4, 5 and 6
        //  - product 61 that is a variant of configurable product 62
        $testedProductIds = [
            self::$db->getProductId('Joust Duffle Bag'), // ID 1
            self::$db->getProductId('Wayfarer Messenger Bag'), // ID 4
            self::$db->getProductId('Chaz Kangeroo Hoodie-XL-Gray'), // ID 60
            self::$db->getProductId('Chaz Kangeroo Hoodie-XL-Orange'), // ID 61
            self::$db->getProductId('Chaz Kangeroo Hoodie') // ID 62
        ];
        $testedStoreIds = [self::$store1Id, self::$store2Id, self::$website2StoreId];

        // when: perform any change of products - to trigger collecting their IDs by the mView feature. A good sample change is to make sure all are visible in the stores
        foreach ($testedStoreIds as $storeId) {
            self::$db->setProductsVisibleInStore($storeId, ...$testedProductIds);
        }

        $expectedPublishedKeys = [
            self::STORE_1_CODE . '_product:1',
            self::STORE_1_CODE . '_product:4',
            self::STORE_1_CODE . '_product:60',
            self::STORE_1_CODE . '_product:61',
            self::STORE_1_CODE . '_product:62', // note: editing parent product is expected to trigger publishing also all its variants

            self::STORE_2_CODE . '_product:1',
            self::STORE_2_CODE . '_product:4',
            self::STORE_2_CODE . '_product:60',
            self::STORE_2_CODE . '_product:61',
            self::STORE_2_CODE . '_product:62',

            self::WEBSITE_2_STORE_CODE . '_product:4',
            self::WEBSITE_2_STORE_CODE . '_product:61',
            self::WEBSITE_2_STORE_CODE . '_product:62',
        ];

        $unexpectedPublishedKeys = [
            self::WEBSITE_2_STORE_CODE . '_product:1', // those products are not available in the second website
            self::WEBSITE_2_STORE_CODE . '_product:59',
            self::WEBSITE_2_STORE_CODE . '_product:60'
        ];

        // and: test store-level attribute labels
        $labelId = $this->addStoreLevelLabelForStyleBagsAttribute('style_bags', self::$store2Id, 'Overridden label for Style Bags');

        // and
        $this->removeFromStreamX(...$expectedPublishedKeys, ...$unexpectedPublishedKeys);
        ConfigurationEditUtils::addIndexedProductAttributes('style_bags');

        try {
            // when
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished(self::STORE_1_CODE . '_product:1', 'original-bag-product.json');
            $this->assertExactDataIsPublished(self::STORE_1_CODE . '_product:4', 'wayfarer-bag-product.json',  [
                '"label":"Style"' => '"label":"Style Bags"' // test database contains an overridden label for this attribute for default store (ID=1)
            ]);
            $this->assertExactDataIsPublished(self::STORE_1_CODE . '_product:60', 'original-hoodie-xl-gray-product.json');
            $this->assertExactDataIsPublished(self::STORE_1_CODE . '_product:61', 'original-hoodie-xl-orange-product.json');
            $this->assertExactDataIsPublished(self::STORE_1_CODE . '_product:62', 'original-hoodie-product.json');

            $this->assertExactDataIsPublished(self::STORE_2_CODE . '_product:1', 'original-bag-product.json');
            $this->assertExactDataIsPublished(self::STORE_2_CODE . '_product:4', 'wayfarer-bag-product.json', [
                '"label":"Overridden label for Style Bags"' => '"label":"Style Bags"'
            ]);
            $this->assertExactDataIsPublished(self::STORE_2_CODE . '_product:60', 'original-hoodie-xl-gray-product.json');
            $this->assertExactDataIsPublished(self::STORE_2_CODE . '_product:61', 'original-hoodie-xl-orange-product.json');
            $this->assertExactDataIsPublished(self::STORE_2_CODE . '_product:62', 'original-hoodie-product.json');

            $this->assertExactDataIsPublished(self::WEBSITE_2_STORE_CODE . '_product:4', 'wayfarer-bag-product.json');
            $this->assertExactDataIsPublished(self::WEBSITE_2_STORE_CODE . '_product:61', 'original-hoodie-xl-orange-product.json');
            $this->assertExactDataIsPublished(self::WEBSITE_2_STORE_CODE . '_product:62', 'original-hoodie-product-in-second-website.json');

            // and
            foreach ($unexpectedPublishedKeys as $unexpectedPublishedKey) {
                $this->assertDataIsNotPublished($unexpectedPublishedKey);
            }
        } finally {
            // restore DB changes
            foreach ($testedStoreIds as $storeId) {
                self::$db->unsetProductsVisibleInStore($storeId, ...$testedProductIds);
            }
            ConfigurationEditUtils::restoreDefaultIndexedProductAttributes();
            $this->removeStoreLevelAttributeLabel($labelId);
        }
    }

    private function addStoreLevelLabelForStyleBagsAttribute(string $attributeCode, int $storeId, string $label): int {
        $attributeId = self::$db->getProductAttributeId($attributeCode);
        return self::$db->insert("
            INSERT INTO eav_attribute_label (attribute_id, store_id, value)
                                     VALUES ($attributeId, $storeId, '$label')
        ");
    }

    private function removeStoreLevelAttributeLabel(int $id): void {
        self::$db->execute("DELETE FROM eav_attribute_label WHERE attribute_label_id = $id");
    }

    /** @test */
    public function shouldPublishEnabledAndVisibleProduct() {
        // given: insert two products, with different status/visibility settings
        $sku1 = (string) (new DateTime())->getTimestamp();
        $product1 = self::$db->insertProduct($sku1, self::$website1Id);
        $this->setProductProperties($product1, 'Default name of Product A', Status::STATUS_ENABLED, Visibility::VISIBILITY_NOT_VISIBLE);
        $this->setProductProperties($product1, 'Name of Product A in first store', Status::STATUS_DISABLED, Visibility::VISIBILITY_IN_CATALOG, self::$store1Id);
        $this->setProductProperties($product1, 'Name of Product A in second store', Status::STATUS_ENABLED, Visibility::VISIBILITY_IN_SEARCH, self::$store2Id);
        $product1Id = $product1->getEntityId();

        $sku2 = $sku1.'2';
        $product2 = self::$db->insertProduct($sku2, self::$website1Id);
        $this->setProductProperties($product2, 'Default name of Product B', Status::STATUS_ENABLED, Visibility::VISIBILITY_BOTH);
        $this->setProductProperties($product2, 'Name of Product B in first store', Status::STATUS_ENABLED, Visibility::VISIBILITY_IN_CATALOG, self::$store1Id);
        $this->setProductProperties($product2, 'Name of Product B in second store', Status::STATUS_DISABLED, Visibility::VISIBILITY_NOT_VISIBLE, self::$store2Id);
        $product2Id = $product2->getEntityId();

        // and
        $expectedKeyForProduct1 = self::STORE_2_CODE . "_product:$product1Id";
        $expectedKeyForProduct2 = self::STORE_1_CODE . "_product:$product2Id";

        $unexpectedKeyForProduct1 = self::STORE_1_CODE . "_product:$product1Id";
        $unexpectedKeyForProduct2 = self::STORE_2_CODE . "_product:$product2Id";

        $this->removeFromStreamX($unexpectedKeyForProduct2, $expectedKeyForProduct2, $unexpectedKeyForProduct1, $expectedKeyForProduct1);

        try {
            // when
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished($expectedKeyForProduct1, 'added-minimal-product.json', [
                // provide values for placeholders in the validation file
                $sku1 => 'SKU',
                '"id":"' . $product1Id . '"' => '"id":"123456789"',
                'Name of Product A in second store' => 'PRODUCT_NAME',
                "name-of-product-a-in-second-store-$product1Id" => 'PRODUCT_SLUG',
                'Search' => 'VISIBILITY'
            ]);

            $this->assertExactDataIsPublished($expectedKeyForProduct2, 'added-minimal-product.json', [
                // provide values for placeholders in the validation file
                $sku2 => 'SKU',
                '"id":"' . $product2Id . '"' => '"id":"123456789"',
                'Name of Product B in first store' => 'PRODUCT_NAME',
                "name-of-product-b-in-first-store-$product2Id" => 'PRODUCT_SLUG',
                'Catalog' => 'VISIBILITY'
            ]);

            // and
            $this->assertDataIsNotPublished($unexpectedKeyForProduct1);
            $this->assertDataIsNotPublished($unexpectedKeyForProduct2);
        } finally {
            // and when
            $this->deleteProduct($product1);
            $this->deleteProduct($product2);
            $this->reindexMview();

            // then
            $this->assertDataIsUnpublished($expectedKeyForProduct1);
            $this->assertDataIsUnpublished($expectedKeyForProduct2);
        }
    }

    private function setProductProperties(EntityIds $product, string $name, int $status, int $visibility, int $storeId = self::DEFAULT_STORE_ID): void {
        $nameAttrId = self::$db->getProductAttributeId('name');
        $statusAttrId = self::$db->getProductAttributeId('status');
        $visibilityAttrId = self::$db->getProductAttributeId('visibility');

        self::$db->insertVarcharProductAttribute($product, $nameAttrId, $name, $storeId);
        self::$db->insertIntProductAttribute($product, $statusAttrId, $status, $storeId);
        self::$db->insertIntProductAttribute($product, $visibilityAttrId, $visibility, $storeId);
    }

    private function deleteProduct(EntityIds $productIds): void {
        self::$db->deleteById($productIds->getLinkFieldId(), [
            'catalog_product_entity_int' => self::$db->getEntityAttributeLinkField(),
            'catalog_product_entity_varchar' => self::$db->getEntityAttributeLinkField()
        ]);

        self::$db->deleteById($productIds->getEntityId(), [
            'catalog_product_website' => 'product_id',
            'catalog_product_entity' => 'entity_id'
        ]);

        if (self::$db->isEnterpriseMagento()) {
            self::$db->deleteById($productIds->getEntityId(), [
                'sequence_product' => 'sequence_value'
            ]);
        }
    }
}