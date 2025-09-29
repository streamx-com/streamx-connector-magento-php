<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\AttributeIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;

/**
 * @inheritdoc
 */
class AttributeUpdateTest extends BaseDirectDbEntityUpdateTest {

    const INDEXER_IDS = [AttributeIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishProductThatUsesSimpleAttributeEditedDirectlyInDatabase() {
        $this->shouldPublishProductThatUsesAttributeEditedDirectlyInDatabase(
            'sale',
            'edited-sale-attr-ball-product.json'
        );
    }

    /** @test */
    public function shouldPublishProductThatUsesAttributeWithOptionsEditedDirectlyInDatabase() {
        $this->shouldPublishProductThatUsesAttributeEditedDirectlyInDatabase(
            'material',
            'edited-material-attr-ball-product.json'
        );
    }

    private function shouldPublishProductThatUsesAttributeEditedDirectlyInDatabase(string $attributeCode, string $validationFile): void {
        // given
        $attributeId = self::$db->getProductAttributeId($attributeCode);

        $oldDisplayName = self::$db->getAttributeDisplayName($attributeId);
        $newDisplayName = "Name modified for testing, was $oldDisplayName";

        // and
        $productId = self::$db->getProductId('Dual Handle Cardio Ball'); // this product is known to have both "sale" and "material" attributes
        $expectedKey = self::productKey($productId);
        self::removeFromStreamX($expectedKey);

        // when
        ConfigurationEditUtils::setIndexedProductAttributes('sale', 'material');
        $this->renameAttributeInDb($attributeId, $newDisplayName);

        try {
            // and
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished($expectedKey, $validationFile);
        } finally {
            $this->renameAttributeInDb($attributeId, $oldDisplayName);
            ConfigurationEditUtils::restoreDefaultIndexedProductAttributes();
        }
    }

    /** @test */
    public function shouldPublishProductWithAllAttributes_WhenOneIsEdited_AndEmptyListOfIndexedAttributesIsConfigured(): void {
        // given: prepare any attribute to be renamed, to enable triggering the indexer
        $attributeCode = 'description';
        $attributeId = self::$db->getProductAttributeId($attributeCode);
        $oldDisplayName = self::$db->getAttributeDisplayName($attributeId);
        $newDisplayName = "Name modified for testing, was $oldDisplayName";

        // and
        $productId = self::$db->getProductId('Joust Duffle Bag');
        $expectedKey = self::productKey($productId);
        self::removeFromStreamX($expectedKey);

        // when
        ConfigurationEditUtils::allowIndexingAllProductAttributes();
        $this->renameAttributeInDb($attributeId, $newDisplayName);

        try {
            // and
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished($expectedKey, 'original-bag-product-with-all-atributes.json');
        } finally {
            $this->renameAttributeInDb($attributeId, $oldDisplayName);
            ConfigurationEditUtils::restoreDefaultIndexedProductAttributes();
        }
    }

    /** @test */
    public function shouldNotPublishProduct_WhenItsNotIndexedAttribute_WasEditedDirectlyInDatabase(): void {
        // given
        $attributeCode = 'tax_class_id';
        $attributeId = self::$db->getProductAttributeId($attributeCode);
        $oldDisplayName = self::$db->getAttributeDisplayName($attributeId);
        $newDisplayName = "Name modified for testing, was $oldDisplayName";

        // and
        $productId = self::$db->getProductId('Joust Duffle Bag');
        $expectedKey = self::productKey($productId);
        self::removeFromStreamX($expectedKey);

        try {
            // when
            ConfigurationEditUtils::unsetIndexedProductAttribute($attributeCode);
            $this->renameAttributeInDb($attributeId, $newDisplayName);
            $this->reindexMview();

            // then
            usleep(200_000);
            $this->assertDataIsNotPublished($expectedKey);
        } finally {
            $this->renameAttributeInDb($attributeId, $oldDisplayName);
            ConfigurationEditUtils::restoreDefaultIndexedProductAttributes();
        }
    }

    private function renameAttributeInDb($attributeId, string $newDisplayName): void {
        self::$db->execute("
            UPDATE eav_attribute
               SET frontend_label = '$newDisplayName'
             WHERE attribute_id = $attributeId
        ");
    }
}