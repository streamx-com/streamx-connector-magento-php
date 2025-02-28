<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel\Product;

use StreamX\ConnectorCatalog\Model\ResourceModel\SelectModifierInterface;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Select;
use Magento\Store\Model\StoreManagerInterface;

class CurrentWebsiteSelectModifier implements SelectModifierInterface
{
    private ResourceConnection $resourceConnection;
    private StoreManagerInterface $storeManager;

    public function __construct(
        ResourceConnection $resourceConnection,
        StoreManagerInterface $storeManager
    ) {
        $this->resourceConnection = $resourceConnection;
        $this->storeManager = $storeManager;
    }

    /**
     * {@inheritdoc}
     */
    public function modify(Select $select, int $storeId): void
    {
        $websiteId = (int)$this->storeManager->getStore($storeId)->getWebsiteId();
        $tableName = $this->resourceConnection->getTableName('catalog_product_website');

        $select->join(
            $tableName,
            "$tableName.product_id = entity.entity_id AND $tableName.website_id = $websiteId",
            []
        );
    }

    public function modifyNegate(Select $select, int $storeId): void
    {
        // TODO: Implement modifyNegate() method.
    }
}
