<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel\Product;

use DomainException;
use Exception;
use Magento\Catalog\Model\ResourceModel\Product\Attribute\CollectionFactory;
use StreamX\ConnectorCatalog\Model\ProductMetaData;
use StreamX\ConnectorCatalog\Model\ResourceModel\SelectModifierInterface;
use Magento\Catalog\Model\Product\Attribute\Source\Status;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Select;

class StatusEnabledSelectModifier implements SelectModifierInterface
{
    private ProductMetaData $productMetaData;
    private ResourceConnection $resourceConnection;
    private int $statusAttributeId;
    private string $statusAttributeBackendTable;

    public function __construct(
        ProductMetaData $productMetaData,
        ResourceConnection $resourceConnection,
        CollectionFactory $attributeCollectionFactory
    ) {
        $this->productMetaData = $productMetaData;
        $this->resourceConnection = $resourceConnection;
        $this->loadStatusAttribute($attributeCollectionFactory);
    }

    /**
     * Process the select statement - filter products to select only enabled products
     * @throws Exception
     */
    public function modify(Select $select, int $storeId): void
    {
        $linkField = $this->productMetaData->getLinkField();
        $backendTable = $this->resourceConnection->getTableName($this->statusAttributeBackendTable);

        $select->joinLeft(
            ['d' => $backendTable],
            "d.attribute_id = $this->statusAttributeId AND d.store_id = 0 AND d.$linkField = entity.$linkField",
            []
        )->joinLeft(
            ['c' => $backendTable],
            "c.attribute_id = $this->statusAttributeId AND c.store_id = $storeId AND c.$linkField = entity.$linkField",
            []
        )->where("CASE WHEN c.value_id > 0 THEN c.value = ? ELSE d.value = ? END", Status::STATUS_ENABLED);
    }

    private function loadStatusAttribute(CollectionFactory $attributeCollectionFactory): void
    {
        $attributeCollection = $attributeCollectionFactory
            ->create()
            ->addFieldToFilter('attribute_code', 'status')
            ->setPageSize(1);

        foreach ($attributeCollection as $attribute) {
            $this->statusAttributeId = (int) $attribute->getId();
            $this->statusAttributeBackendTable = $attribute->getBackendTable();
            return;
        }

        throw new DomainException("Cannot load status attribute");
    }
}
