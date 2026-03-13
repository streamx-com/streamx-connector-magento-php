<?php

namespace StreamX\ConnectorCatalog\test\integration\AppEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationKeyPaths;

/**
 * @inheritdoc
 */
class ProductUpdateTest extends BaseAppEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishSimpleProductEditedUsingMagentoApplication() {
        $this->shouldPublishProductEditedUsingMagentoApplication('Joust Duffle Bag', 'bag');

        // and: note: this can be tested only with mesh.yaml (not with mesh-light.yaml) - resource-downloader service is needed
        $this->assertImageIsPublished('/media/catalog/product/m/b/mb01-blue-0.jpg');
    }

    /** @test */
    public function shouldPublishSimpleProductEditedUsingMagentoApplication_WhenRabbitMqIsDisabled() {
        ConfigurationEditUtils::setConfigurationValue(ConfigurationKeyPaths::ENABLE_RABBIT_MQ, '0');
        try {
            $this->shouldPublishProductEditedUsingMagentoApplication('Joust Duffle Bag', 'bag');
        } finally {
            ConfigurationEditUtils::setConfigurationValue(ConfigurationKeyPaths::ENABLE_RABBIT_MQ, '1');
        }
    }

    /** @test */
    public function shouldPublishSimpleProductEditedUsingMagentoApplicationWithoutAttributes() {
        ConfigurationEditUtils::setIndexedProductAttributes('cost'); // index only an attr that bags don't have (so no attr expected in publish payload)
        try {
            $this->shouldPublishProductEditedUsingMagentoApplication('Joust Duffle Bag', 'bag-no-attributes');
        } finally {
            ConfigurationEditUtils::restoreDefaultIndexedProductAttributes();
        }
    }

    /** @test */
    public function shouldPublishGroupedProductEditedUsingMagentoApplication() {
        $regexReplacements = self::$db->isEnterpriseMagento() ? [ // in enterprise magento DB, ID of the grouped product is 45, not 46 as in community version
            '"id":"45",' => '"id":"46",',
            '-45"' => '-46"'
        ] : [];
        $this->shouldPublishProductEditedUsingMagentoApplication('Set of Sprite Yoga Straps', 'grouped', $regexReplacements);
    }

    private function shouldPublishProductEditedUsingMagentoApplication(string $productName, string $productNameInValidationFileName, array $regexReplacements = []): void {
        // given
        $productNewName = "Name modified for testing, was $productName";
        $productId = self::$db->getProductId($productName);

        // and
        $expectedKey = self::productKey($productId);
        self::removeFromStreamX($expectedKey);

        // when
        self::renameProduct($productId, $productNewName);

        // then
        try {
            $this->assertExactDataIsPublished($expectedKey, "edited-$productNameInValidationFileName-product.json", $regexReplacements);
        } finally {
            self::renameProduct($productId, $productName);
            $this->assertExactDataIsPublished($expectedKey, "original-$productNameInValidationFileName-product.json", $regexReplacements);
        }
    }

    /** @test */
    public function shouldNotPublishBundleProduct() {
        // given: publish some dummy data directly at the product key, to later on verify that editing the product doesn't result in publishing it via indexer
        $productName = 'Sprite Yoga Companion Kit';
        $productId = self::$db->getProductId($productName);

        // and
        $productToPublish = ['id' => (string) $productId->getEntityId()];
        $streamxClient = parent::createStreamxClient();
        $store = parent::createStoreMock(self::$store1Id, self::STORE_1_CODE);
        $streamxClient->publish([$productToPublish], ProductIndexer::INDEXER_ID, $store);

        // verify published
        $expectedKey = self::productKey($productId);
        $regexReplacements = self::$db->isEnterpriseMagento() ? [ '46' => '45' ] : []; // in enterprise magento DB, ID of the bundle product is 46, not 45 as in community version
        $this->assertExactDataIsPublished($expectedKey, "dummy-bundle-product.json", $regexReplacements);

        // when
        $productNewName = "Name modified for testing, was $productName";
        self::renameProduct($productId, $productNewName);

        try {
            // then: due to bundle products being not available in configuration - their IDs are treated as not existing by the indexer, so are unpublished
            $this->assertDataIsUnpublished($expectedKey);
        } finally {
            // and when: restore original name
            self::renameProduct($productId, $productName);

            // then: still nothing is published at the key
            $this->assertDataIsNotPublished($expectedKey);
        }
    }
}