<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Indexer;

use Psr\Log\LoggerInterface;
use StreamX\ConnectorCatalog\Model\Attributes\AttributeDefinition;
use StreamX\ConnectorCatalog\Model\Indexer\DataLoader\AttributeDataLoader;
use StreamX\ConnectorCatalog\Model\Indexer\AttributeProcessor;
use StreamX\ConnectorCatalog\Model\Indexer\DataLoader\ProductDataLoader;
use StreamX\ConnectorCatalog\Model\ResourceModel\Product;
use StreamX\ConnectorCore\Api\IndexersConfigInterface;
use StreamX\ConnectorCore\Config\OptimizationSettings;
use StreamX\ConnectorCore\Indexer\BaseStreamxIndexer;
use StreamX\ConnectorCore\Indexer\IndexableStoresProvider;
use StreamX\ConnectorCore\Client\StreamxClient;
use StreamX\ConnectorCore\Client\StreamxClientConfiguration;
use StreamX\ConnectorCore\System\GeneralConfig;
use Traversable;

// TODO implement checking if only relevant attribute properties have changed to trigger publishing products
class AttributesIndexer extends BaseStreamxIndexer
{
    private Product $productModel;
    private ProductsIndexer $productsIndexer;
    private ProductDataLoader $productDataLoader;

    public function __construct(
        GeneralConfig $connectorConfig,
        IndexableStoresProvider $indexableStoresProvider,
        AttributeDataLoader $dataLoader,
        LoggerInterface $logger,
        OptimizationSettings $optimizationSettings,
        StreamxClientConfiguration $clientConfiguration,
        IndexersConfigInterface $indexersConfig,
        Product $productModel,
        ProductsIndexer $productsIndexer,
        ProductDataLoader $productDataLoader
    ) {
        parent::__construct(
            $connectorConfig,
            $indexableStoresProvider,
            $dataLoader,
            $logger,
            $optimizationSettings,
            $clientConfiguration,
            $indexersConfig->getByName(AttributeProcessor::INDEXER_ID)
        );
        $this->productModel = $productModel;
        $this->productsIndexer = $productsIndexer;
        $this->productDataLoader = $productDataLoader;
    }

    /**
     * Override to instead of publishing attributes -> publish products that use those attributes
     * @param Traversable<AttributeDefinition> $attributeDefinitions
     */
    public function ingestEntities(Traversable $attributeDefinitions, int $storeId, StreamxClient $client): void {
        $changedAttributeIds = [];

        /** @var $attributeDefinition AttributeDefinition */
        foreach ($attributeDefinitions as $attributeDefinition) {
            if ($attributeDefinition === null) {
                // a deleted attribute. Currently, no way of collecting products that used it before
                continue;
            }
            $this->logger->info("Definition of attribute '{$attributeDefinition->getCode()}' has changed");
            $changedAttributeIds[] = $attributeDefinition->getId();
        }

        $productIds = $this->productModel->loadIdsOfProductsThatUseAttributes($changedAttributeIds, $storeId);
        if (empty($productIds)) {
            // no changes in the attributes that should cause republishing products
            return;
        }

        $this->logger->info("Detected the following products to re-publish due to attribute definition change: " . json_encode($productIds));
        $products = $this->productDataLoader->loadData($storeId, $productIds);
        $this->productsIndexer->ingestEntities($products, $storeId, $client);
    }
}