<?php

namespace StreamX\ConnectorCatalog\test\integration\AppEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\test\integration\utils\CodeCoverageReportGenerator;

/**
 * @inheritdoc
 * @UsesAttributeIndexer
 */
class AttributeUpdateTest extends BaseAppEntityUpdateTest {

    /** @test */
    public function shouldPublishProductThatUsesSimpleAttributeEditedUsingMagentoApplicationToStreamx() {
        $this->shouldPublishProductThatUsesAttributeEditedUsingMagentoApplicationToStreamx(
            'sale',
            ['"label": "Sale"' => '"label": "Name modified for testing, was Sale"']
        );
    }

    /** @test */
    public function shouldPublishProductThatUsesAttributeWithOptionsEditedUsingMagentoApplicationToStreamx() {
        $this->shouldPublishProductThatUsesAttributeEditedUsingMagentoApplicationToStreamx(
            'material',
            ['"label": "Material"' => '"label": "Name modified for testing, was Material"']
        );
    }

    private function shouldPublishProductThatUsesAttributeEditedUsingMagentoApplicationToStreamx(string $attributeCode, array $regexReplacementsForEditedValidationFile): void {
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

        self::renameAttribute($attributeCode, $newDisplayName);

        // then
        try {
            $this->assertExactDataIsPublished($expectedKey, "edited-ball-product.json", $regexReplacementsForEditedValidationFile);
        } finally {
            try {
                self::renameAttribute($attributeCode, $oldDisplayName);
                // TODO original and edited products have the same attribute name, the original attribute name. Attribute name change is not reflected in edited-ball-product.json
                $this->assertExactDataIsPublished($expectedKey, "original-ball-product.json");
            } finally {
                $this->restoreConfigurationValue($this->PRODUCT_ATTRIBUTES_PATH);
            }
        }
    }

    private function renameAttribute(string $attributeCode, string $newName): void {
        $coverage = self::callMagentoPutEndpoint('attribute/rename', [
            'attributeCode' => $attributeCode,
            'newName' => $newName
        ]);

        if (getenv('GENERATE_CODE_COVERAGE_REPORT') === 'true') {
            CodeCoverageReportGenerator::generateCodeCoverageReport($coverage, $this);
        }
    }
}