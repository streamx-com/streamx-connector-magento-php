<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel\Product;

use DomainException;
use Exception;
use Magento\Catalog\Model\Product\Visibility;
use Magento\Catalog\Model\ResourceModel\Product\Attribute\CollectionFactory;
use StreamX\ConnectorCatalog\Model\ProductMetaData;
use StreamX\ConnectorCatalog\Model\ResourceModel\SelectModifierInterface;
use Magento\Catalog\Model\Product\Attribute\Source\Status;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Select;
use StreamX\ConnectorCatalog\Model\SystemConfig\CatalogConfig;

class VisibleSelectModifier implements SelectModifierInterface
{
    private ProductMetaData $productMetaData;
    private ResourceConnection $resourceConnection;
    private CatalogConfig $catalogConfig;
    private int $visibilityAttributeId;
    private string $visibilityAttributeBackendTable;

    public function __construct(
        ProductMetaData $productMetaData,
        ResourceConnection $resourceConnection,
        CatalogConfig $catalogConfig,
        CollectionFactory $attributeCollectionFactory
    ) {
        $this->productMetaData = $productMetaData;
        $this->resourceConnection = $resourceConnection;
        $this->catalogConfig = $catalogConfig;
        $this->loadVisibilityAttribute($attributeCollectionFactory);
    }

    /**
     * Process the select statement - filter products to select only visible products
     * @throws Exception
     */
    public function modify(Select $select, int $storeId): void
    {
        if ($this->catalogConfig->shouldExportProductsNotVisibleIndividually()) {
            return;
        }

        $linkField = $this->productMetaData->getLinkField();
        $backendTable = $this->resourceConnection->getTableName($this->visibilityAttributeBackendTable);

        $select->joinLeft(
            ['d2' => $backendTable],
            "d2.attribute_id = $this->visibilityAttributeId AND d2.store_id = 0 AND d2.$linkField = entity.$linkField",
            []
        )->joinLeft(
            ['c2' => $backendTable],
            "c2.attribute_id = $this->visibilityAttributeId AND c2.store_id = $storeId AND c2.$linkField = entity.$linkField",
            []
        )->where("CASE WHEN c2.value_id > 0 THEN c2.value <> ? ELSE d2.value <> ? END", Visibility::VISIBILITY_NOT_VISIBLE);
    }

    private function loadVisibilityAttribute(CollectionFactory $attributeCollectionFactory): void
    {
        $attributeCollection = $attributeCollectionFactory
            ->create()
            ->addFieldToFilter('attribute_code', 'visibility')
            ->setPageSize(1);

        foreach ($attributeCollection as $attribute) {
            $this->visibilityAttributeId = (int) $attribute->getId();
            $this->visibilityAttributeBackendTable = $attribute->getBackendTable();
            return;
        }

        throw new DomainException("Cannot load visibility attribute");
    }

    public function modifyNegate(Select $select, int $storeId): void
    {
        // TODO: Implement modifyNegate() method.
    }
}
