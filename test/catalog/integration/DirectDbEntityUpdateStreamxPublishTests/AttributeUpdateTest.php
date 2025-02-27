<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

/**
 * @inheritdoc
 * @UsesAttributeIndexer
 */
class AttributeUpdateTest extends BaseDirectDbEntityUpdateTest {

    /** @test */
    public function shouldPublishProductThatUsesSimpleAttributeEditedDirectlyInDatabaseToStreamx() {
        $this->shouldPublishProductThatUsesAttributeEditedDirectlyInDatabaseToStreamx(
            'sale',
            ['"label": "Sale"' => '"label": "Name modified for testing, was Sale"']
        );
    }

    /** @test */
    public function shouldPublishProductThatUsesAttributeWithOptionsEditedDirectlyInDatabaseToStreamx() {
        $this->shouldPublishProductThatUsesAttributeEditedDirectlyInDatabaseToStreamx(
            'material',
            ['"label": "Material"' => '"label": "Name modified for testing, was Material"']
        );
    }

    private function shouldPublishProductThatUsesAttributeEditedDirectlyInDatabaseToStreamx(string $attributeCode, array $regexReplacementsForEditedValidationFile): void {
        // given
        $attributeId = self::$db->getProductAttributeId($attributeCode);

        $oldDisplayName = self::$db->getAttributeDisplayName($attributeId);
        $newDisplayName = "Name modified for testing, was $oldDisplayName";

        // and
        $productId = self::$db->getProductId('Dual Handle Cardio Ball'); // this product is known to have both "sale" and "material" attributes
        $expectedKey = "default_product:$productId";
        self::removeFromStreamX($expectedKey);

        // when
        $this->setConfigurationValue($this->PRODUCT_ATTRIBUTES_PATH, 'sale,material');

        $this->renameAttributeInDb($attributeId, $newDisplayName);

        try {
            // and
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished($expectedKey, "edited-ball-product.json", $regexReplacementsForEditedValidationFile);
        } finally {
            try {
                $this->renameAttributeInDb($attributeId, $oldDisplayName);
            } finally {
                $this->restoreConfigurationValue($this->PRODUCT_ATTRIBUTES_PATH);
            }
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