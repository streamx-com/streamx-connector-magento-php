<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel;

use StreamX\ConnectorCatalog\Model\ProductMetaData;
use Magento\Framework\App\ResourceConnection;

class ProductConfig
{
    private ResourceConnection $resource;
    private ProductMetaData $productMetaData;
    private ?int $entityTypeId = null;

    public function __construct(
        ProductMetaData $productMetaData,
        ResourceConnection $resourceConnection
    ) {
        $this->resource = $resourceConnection;
        $this->productMetaData = $productMetaData;
    }

    public function getEntityTypeId(): int
    {
        if (null === $this->entityTypeId) {
            $entityTypeCode = $this->productMetaData->getEavEntityType();

            $connection = $this->resource->getConnection();
            $select = $connection->select()->from(
                $this->resource->getTableName('eav_entity_type'),
                ['entity_type_id']
            )->where('entity_type_code = ?', $entityTypeCode);

            $this->entityTypeId = (int) $connection->fetchOne($select);
        }

        return $this->entityTypeId;
    }
}
