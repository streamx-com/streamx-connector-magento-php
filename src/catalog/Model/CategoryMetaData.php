<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model;

use Magento\Catalog\Api\Data\CategoryInterface;
use Magento\Framework\EntityManager\MetadataPool;

class CategoryMetaData {

    private string $entityTable;
    private string $entityIdField;
    private string $linkField;

    public function __construct(MetadataPool $metadataPool) {
        $categoryMetaData = $metadataPool->getMetadata(CategoryInterface::class);
        $this->entityTable = $categoryMetaData->getEntityTable();
        $this->entityIdField = $categoryMetaData->getIdentifierField();
        $this->linkField = $categoryMetaData->getLinkField();
    }

    public function getEntityTable(): string {
        return $this->entityTable;
    }

    public function getEntityIdField(): string {
        return $this->entityIdField;
    }

    public function getLinkField(): string {
        return $this->linkField;
    }
}
