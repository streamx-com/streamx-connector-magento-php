<?php

namespace StreamX\ConnectorCatalog\test\integration;

use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\ValidationFileUtils;

class StreamxConnectorClientLoadTest extends BaseStreamxTest {

    use ValidationFileUtils;

    private const STORE_ID = 1;
    private const STORE_CODE = 'store_1';

    private const NOT_EXISTING_HOST = 'c793qwh0uqw3fg94ow';
    private const WRONG_INGESTION_PORT = 1234;

    /** @test */
    public function shouldPublishBigBatchesOfProductsWithoutErrors() {
        // given
        $bigProductJson = $this->readValidationFileContent('original-hoodie-product.json');
        $entity = json_decode($bigProductJson, true);

        $entitiesToPublishInBatch = 100;

        // and: load the same big product to list, but give each instance a unique ID
        $entities = [];
        for ($i = 0; $i < $entitiesToPublishInBatch; $i++) {
            $entities[] = $entity;
            $entities[$i]['id'] = strval($i);
        }

        self::removeFromStreamX(...array_map(function (int $id) {
            return self::expectedStreamxProductKey($id);
        }, array_keys($entities)));

        // when: publish batch as the Connector would do
        $client = parent::createStreamxClient();
        $store = parent::createStoreMock(self::STORE_ID, self::STORE_CODE);
        $client->publish($entities, ProductIndexer::INDEXER_ID, $store);

        // then
        for ($i = $entitiesToPublishInBatch - 1; $i >= 0; $i--) {
            $this->assertExactDataIsPublished(self::expectedStreamxProductKey($i), 'original-hoodie-product.json', [
                '"sku":"MH01","id":"'. $i . '",' => '"sku":"MH01","id":"62",' // 62 is the product ID in validation file
            ]);
        }

        // and when: unpublish
        $client->unpublish(array_column($entities, 'id'), ProductIndexer::INDEXER_ID, $store);

        // then
        for ($i = 0; $i < $entitiesToPublishInBatch; $i++) {
            $this->assertDataIsUnpublished(self::expectedStreamxProductKey($i));
        }
    }

    private static function expectedStreamxProductKey(int $productId): string {
        return BaseStreamxConnectorPublishTest::productKeyFromEntityId($productId, self::STORE_CODE);
    }
}