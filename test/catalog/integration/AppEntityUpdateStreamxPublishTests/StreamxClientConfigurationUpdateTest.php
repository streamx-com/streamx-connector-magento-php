<?php

namespace StreamX\ConnectorCatalog\test\integration\AppEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationKeyPaths;

/**
 * @inheritdoc
 */
class StreamxClientConfigurationUpdateTest extends BaseAppEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    /** @test */
    public function shouldImmediatelyUseUpdatedStreamxClientConfiguration() {
        // given
        $productName = 'Joust Duffle Bag';
        $productId = self::$db->getProductId($productName);

        // and
        $expectedKey = self::productKey($productId);
        self::removeFromStreamX($expectedKey);

        try {
            // when 1: trigger publishing product by renaming it
            self::renameProduct($productId, "Name modified for testing, was $productName");

            // then
            $this->assertExactDataIsPublished($expectedKey, "edited-bag-product.json");

            // when 2: edit configuration to point to another StreamX instance and rename the product again
            ConfigurationEditUtils::setConfigurationValue(ConfigurationKeyPaths::INGESTION_BASE_URL, 'http://localhost:9999');
            self::renameProduct($productId, "Name modified again, was $productName");

            // then: expect the product data to be sent to the edited (unreachable) StreamX host...
            sleep(1);
            $this->logFileUtils->verifyLogged(
                'ERROR: Error response from StreamX to the message: {"isBatch":false,"storeId":1,"cloudEvents":{"specversion":"1.0","id":',
                'WARNING: Retrying the message by republishing with routing key ingestion-requests-retry-1'
            );

            // ...and remain unchanged in the original destination
            $this->assertExactDataIsPublished($expectedKey, "edited-bag-product.json");

            // when 3: restore settings
            ConfigurationEditUtils::restoreConfigurationValue(ConfigurationKeyPaths::INGESTION_BASE_URL);

            // then: expect the edited data to eventually reach StreamX
            $this->assertExactDataIsPublished($expectedKey, "bag-product-edited-again.json");
        } finally {
            // restore changes
            ConfigurationEditUtils::restoreConfigurationValue(ConfigurationKeyPaths::INGESTION_BASE_URL);
            self::renameProduct($productId, $productName);
        }
    }
}