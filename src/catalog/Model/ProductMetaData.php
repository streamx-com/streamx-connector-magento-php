<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model;

use Magento\Catalog\Api\Data\ProductInterface;
use Magento\Framework\EntityManager\MetadataPool;

class ProductMetaData {

    private string $entityTable;
    private string $identifierField;
    private string $linkField;
    private string $eavEntityType;

    public function __construct(MetadataPool $metadataPool) {
        $productMetaData = $metadataPool->getMetadata(ProductInterface::class);
        $this->entityTable = $productMetaData->getEntityTable();
        $this->identifierField = $productMetaData->getIdentifierField();
        $this->linkField = $productMetaData->getLinkField();
        $this->eavEntityType = $productMetaData->getEavEntityType();
    }

    public function getEntityTable(): string {
        return $this->entityTable;
    }

    public function getIdentifierField(): string {
        return $this->identifierField;
    }

    public function getLinkField(): string {
        return $this->linkField;
    }

    public function getEavEntityType(): string {
        return $this->eavEntityType;
    }
}
