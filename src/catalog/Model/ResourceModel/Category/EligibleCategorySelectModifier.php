<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel\Category;

use DomainException;
use Exception;
use Magento\Catalog\Model\Category;
use Magento\Catalog\Model\ResourceModel\Category\Attribute\CollectionFactory;
use Magento\Store\Model\StoreManagerInterface;
use StreamX\ConnectorCatalog\Model\CategoryMetaData;
use StreamX\ConnectorCatalog\Model\ResourceModel\SelectModifierInterface;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Select;

class EligibleCategorySelectModifier implements SelectModifierInterface
{
    private CategoryMetaData $categoryMetadata;
    private ResourceConnection $resourceConnection;
    private StoreManagerInterface $storeManager;
    private int $isActiveAttributeId;
    private string $isActiveAttributeBackendTable;

    public function __construct(
        CategoryMetaData $metadataPool,
        ResourceConnection $resourceConnection,
        StoreManagerInterface $storeManager,
        CollectionFactory $attributeCollectionFactory
    ) {
        $this->categoryMetadata = $metadataPool;
        $this->resourceConnection = $resourceConnection;
        $this->storeManager = $storeManager;
        $this->loadIsActiveAttribute($attributeCollectionFactory);
    }

    public function modify(Select $select, int $storeId): void {
        $this->addConditions($select, $storeId, true);
    }

    public function modifyNegate(Select $select, int $storeId): void {
        $this->addConditions($select, $storeId, false);
    }

    private function addConditions(Select $select, int $storeId, bool $isActiveAndFromCurrentStore): void {
        $linkField = $this->categoryMetadata->getLinkField();
        $backendTable = $this->resourceConnection->getTableName($this->isActiveAttributeBackendTable);

        $select->joinLeft(
            ['d' => $backendTable],
            "d.attribute_id = $this->isActiveAttributeId AND d.store_id = 0 AND d.$linkField = entity.$linkField",
            []
        )->joinLeft(
            ['c' => $backendTable],
            "c.attribute_id = $this->isActiveAttributeId AND c.store_id = $storeId AND c.$linkField = entity.$linkField",
            []
        );

        if ($isActiveAndFromCurrentStore) {
            $activeCondition = "CASE WHEN c.value_id > 0 THEN c.value = 1 ELSE d.value = 1 END";
            $fromStoreCondition = $this->categoryPathCondition($storeId);
            $select->where("(($activeCondition) AND ($fromStoreCondition))");
        } else {
            $notActiveCondition = "CASE WHEN c.value_id > 0 THEN c.value = 0 ELSE d.value = 0 END";
            $notFromStoreCondition = "NOT(" . $this->categoryPathCondition($storeId) . ")";
            $select->where("(($notActiveCondition) OR ($notFromStoreCondition))");
        }
    }

    private function loadIsActiveAttribute(CollectionFactory $attributeCollectionFactory): void {
        $attributeCollection = $attributeCollectionFactory
            ->create()
            ->addFieldToFilter('attribute_code', 'is_active')
            ->setPageSize(1);

        foreach ($attributeCollection as $attribute) {
            $this->isActiveAttributeId = (int) $attribute->getId();
            $this->isActiveAttributeBackendTable = $attribute->getBackendTable();
            return;
        }

        throw new DomainException("Cannot load is_active attribute");
    }

    private function categoryPathCondition(int $storeId): string {
        $store = $this->storeManager->getStore($storeId);
        $rootCategoryPath = Category::TREE_ROOT_ID . "/" . $store->getRootCategoryId();
        return "path = '$rootCategoryPath' OR path like '$rootCategoryPath/%'";
    }
}
