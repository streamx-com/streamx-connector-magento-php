<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Indexer;

use Magento\Store\Api\Data\StoreInterface;
use StreamX\ConnectorCatalog\Model\Attribute\IndexableAttributesFilter;
use StreamX\ConnectorCatalog\Model\Attributes\AttributeDefinition;
use StreamX\ConnectorCatalog\Model\Indexer\DataLoader\AttributeDataLoader;
use StreamX\ConnectorCatalog\Model\Indexer\DataLoader\ProductDataLoader;
use StreamX\ConnectorCatalog\Model\ResourceModel\Product;
use StreamX\ConnectorCore\Indexer\BaseStreamxIndexer;
use StreamX\ConnectorCore\Indexer\StreamxIndexerServices;
use Traversable;

class AttributeIndexer extends BaseStreamxIndexer {

    public const INDEXER_ID = 'streamx_attribute_indexer';

    private IndexableAttributesFilter $indexableAttributesFilter;
    private Product $productModel;
    private ProductIndexer $productIndexer;
    private ProductDataLoader $productDataLoader;

    public function __construct(
        StreamxIndexerServices $indexerServices,
        AttributeDataLoader $dataLoader,
        IndexableAttributesFilter $indexableAttributesFilter,
        Product $productModel,
        ProductIndexer $productIndexer,
        ProductDataLoader $productDataLoader
    ) {
        parent::__construct($indexerServices, $dataLoader);
        $this->indexableAttributesFilter = $indexableAttributesFilter;
        $this->productModel = $productModel;
        $this->productIndexer = $productIndexer;
        $this->productDataLoader = $productDataLoader;
    }

    /**
     * Override to instead of publishing attributes -> publish products that use those attributes
     * @param Traversable<AttributeDefinition> $attributeDefinitions
     */
    protected function ingestEntities(Traversable $attributeDefinitions, StoreInterface $store): void {
        $addedOrEditedAttributes = [];

        /** @var $attributeDefinition AttributeDefinition */
        foreach ($attributeDefinitions as $attributeDefinition) {
            if ($attributeDefinition !== null) {
                $addedOrEditedAttributes[] = $attributeDefinition;
            } // else: a deleted attribute. Reindexing products that used it is handled in UpdateAttributeDataPlugin
        }

        $storeId = (int) $store->getId();
        $indexableProductAttributeIds = $this->indexableAttributesFilter->filterIdsOfIndexableProductAttributes($addedOrEditedAttributes, $storeId);
        $indexableChildProductAttributeIds = $this->indexableAttributesFilter->filterIdsOfIndexableChildProductAttributes($addedOrEditedAttributes, $storeId);

        $productIds = [];
        array_push($productIds, ...$this->productModel->loadIdsOfProductsThatUseAttributes($indexableProductAttributeIds, $storeId));
        array_push($productIds, ...$this->productModel->loadIdsOfChildProductsThatUseAttributes($indexableChildProductAttributeIds, $storeId));

        if (!empty($productIds)) {
            // TODO to be considered: check if only relevant attribute properties have changed to trigger publishing products (only changes in code, label, isFacet and options should matter)
            $this->logger->info("Detected the following products to re-publish due to attribute definition change: " . json_encode($productIds));

            $products = $this->productDataLoader->loadData($storeId, $productIds);
            $products = $this->filterProductsToPublish($products);
            $this->productIndexer->ingestEntities($products, $store);
        }
    }

    private function filterProductsToPublish(Traversable $products): Traversable {
        foreach ($products as $id => $product) {
            if ($product) {
                yield $id => $product;
            }
        }
    }
}