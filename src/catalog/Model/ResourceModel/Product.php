<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel;

use Exception;
use Magento\Framework\DB\Adapter\AdapterInterface;
use StreamX\ConnectorCatalog\Model\ResourceModel\Product\StatusEnabledSelectModifier;
use StreamX\ConnectorCatalog\Model\ResourceModel\Product\CurrentWebsiteSelectModifier;
use StreamX\ConnectorCatalog\Model\ResourceModel\Product\VisibleSelectModifier;
use StreamX\ConnectorCatalog\Model\SystemConfig\CatalogConfig;
use StreamX\ConnectorCatalog\Model\ProductMetaData;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Helper as DbHelper;
use Magento\Framework\DB\Select;
use Zend_Db_Expr;
use Zend_Db_Select_Exception;

class Product
{
    private const PRODUCT_ATTRIBUTE_TABLES = [
        'catalog_product_entity_datetime',
        'catalog_product_entity_decimal',
        'catalog_product_entity_gallery',
        'catalog_product_entity_int',
        'catalog_product_entity_text',
        'catalog_product_entity_varchar'
    ];

    private ResourceConnection $resourceConnection;
    private DbHelper $dbHelper;
    private CatalogConfig $productSettings;
    private ?array $configurableAttributeIds = null;
    private ProductMetaData $productMetaData;
    private CompositeSelectModifier $websiteAndStatusAndVisibleSelectModifier;
    private CompositeSelectModifier $websiteAndStatusSelectModifier;

    public function __construct(
        CatalogConfig $configSettings,
        CurrentWebsiteSelectModifier $currentWebsiteSelectModifier,
        StatusEnabledSelectModifier $statusEnabledSelectModifier,
        VisibleSelectModifier $visibleSelectModifier,
        ResourceConnection $resourceConnection,
        ProductMetaData $productMetaData,
        DbHelper $dbHelper
    ) {
        $this->resourceConnection = $resourceConnection;
        $this->dbHelper = $dbHelper;
        $this->productSettings = $configSettings;
        $this->websiteAndStatusAndVisibleSelectModifier = new CompositeSelectModifier($currentWebsiteSelectModifier, $statusEnabledSelectModifier, $visibleSelectModifier);
        $this->websiteAndStatusSelectModifier = new CompositeSelectModifier($currentWebsiteSelectModifier, $statusEnabledSelectModifier);
        $this->productMetaData = $productMetaData;
    }

    /**
     * @throws Exception
     */
    public function getProducts(int $storeId, array $productIds, int $fromId, int $limit = 1000): array
    {
        $entityIdColumn = "entity.entity_id";

        $select = $this
            ->prepareProductSelect($this->getRequiredColumns(), $storeId)
            ->where("$entityIdColumn > ?", $fromId)
            ->limit($limit);

        if (!empty($productIds)) {
            $select->where("$entityIdColumn IN (?)", $productIds);
        }

        return $this->getConnection()->fetchAll($select);
    }

    /**
     * @return int[]
     */
    public function getAllProductIds(int $storeId): array
    {
        $select = $this->prepareProductSelect(['entity_id'], $storeId);
        return $this->getConnection()->fetchCol($select);
    }

    /**
     * Prepares product select for selecting main products
     */
    private function prepareProductSelect(array $columns, int $storeId): Select {
        $select = $this->prepareBaseProductSelect($columns, $storeId, $this->websiteAndStatusAndVisibleSelectModifier);
        $this->addProductTypeFilter($select);
        $select->order('entity.entity_id ASC');
        return $select;
    }

    /**
     * Prepares base product select for selecting main or variant products
     */
    private function prepareBaseProductSelect(array $requiredColumns, int $storeId, CompositeSelectModifier $selectModifier): Select
    {
        $select = $this->getConnection()->select()
            ->from(
                ['entity' => $this->productMetaData->getEntityTable()],
                $requiredColumns
            );

        $selectModifier->modifyAll($select, $storeId);

        return $select;
    }

    private function getRequiredColumns(): array
    {
        $columns = [
            'entity_id',
            'type_id',
            'sku',
        ];

        $linkField = $this->productMetaData->getLinkField();

        if ($this->productMetaData->getIdentifierField() !== $linkField) {
            $columns[] = $linkField;
        }

        return $columns;
    }

    /**
     */
    public function loadChildrenProducts(array $parentIds, int $storeId): array
    {
        $linkField = $this->productMetaData->getLinkField();
        $entityId = $this->productMetaData->getIdentifierField();
        $columns = [
            'sku',
            $entityId,
        ];

        if ($linkField !== $entityId) {
            $columns[] = $linkField;
        }

        // when loading children (variants) for a configurable product, don't filter by visibility
        $select = $this->prepareBaseProductSelect($columns, $storeId, $this->websiteAndStatusSelectModifier);

        $select->join(
            ['link_table' => $this->resourceConnection->getTableName('catalog_product_super_link')],
            'link_table.product_id = entity.entity_id',
            []
        );

        $select->where('link_table.parent_id IN (?)', $parentIds);
        $select->group('entity_id');

        $this->dbHelper->addGroupConcatColumn($select, 'parent_ids', 'parent_id');

        return $this->getConnection()->fetchAll($select);
    }

    private function addProductTypeFilter(Select $select): void
    {
        $types = $this->productSettings->getAllowedProductTypes();
        $select->where('entity.type_id IN (?)', $types);
    }

    /**
     * @param int[] $attributeIds
     * @return int[]
     * @throws Zend_Db_Select_Exception
     */
    // TODO add conditions for products only from current website / active / visible
    public function loadIdsOfProductsThatUseAttributes(array $attributeIds, int $storeId): array
    {
        if (empty($attributeIds)) {
            return [];
        }

        $linkField = $this->productMetaData->getLinkField();
        $connection = $this->resourceConnection->getConnection();

        $selectProductIdsQueries = [];
        foreach (self::PRODUCT_ATTRIBUTE_TABLES as $table) {
            // select values of all [row_id] or [entity_id] from each product attributes table
            $selectProductIdsQueries[] = $connection->select()
                ->from($this->resourceConnection->getTableName($table), [$linkField])
                ->distinct()
                ->where('attribute_id IN(?)', $attributeIds);
        }

        // union results of all the selects
        $selectProductIdsUnionQuery = $connection->select()->union($selectProductIdsQueries);

        // select actual entity_ids from main products table, that have the product ids found in all the product attribute tables
        $selectProductEntityIds = $connection->select()
            ->from(['entity' => $this->productMetaData->getEntityTable()], ['entity_id'])
            ->distinct()
            ->where("entity.$linkField IN(?)", new Zend_Db_Expr($selectProductIdsUnionQuery))
            ->order('entity_id');

        $this->websiteAndStatusAndVisibleSelectModifier->modifyAll($selectProductEntityIds, $storeId);

        return $connection->fetchCol($selectProductEntityIds);
    }

    /**
     * @param int[] $productIds
     * @return int[]
     */
    public function retrieveParentsForVariants(array $productIds, int $storeId): array
    {
        /** The base query (for community DB version) is:
         * SELECT DISTINCT entity.entity_id AS parentId
         *   FROM catalog_product_entity entity
         *   JOIN catalog_product_relation relation ON relation.parent_id = entity.entity_id
         *  WHERE entity.type_id = 'configurable'
         *    AND relation.child_id IN ($productIds)
         *  ORDER BY entity.entity_id
         */
        $linkFieldId = $this->productMetaData->getLinkField();
        $productIdField = $this->productMetaData->getIdentifierField();
        $entityTable = $this->resourceConnection->getTableName($this->productMetaData->getEntityTable());
        $relationTable = $this->resourceConnection->getTableName('catalog_product_relation');
        $productIdsString = implode(',', $productIds);

        $select = $this->getConnection()->select()
            ->from(['entity' => $entityTable], $productIdField)
            ->join(['relation' => $relationTable], "relation.parent_id = entity.$linkFieldId", [])
            ->where("entity.type_id = 'configurable'")
            ->where("relation.child_id IN($productIdsString)");

        $this->websiteAndStatusAndVisibleSelectModifier->modifyAll($select, $storeId);

        return array_map('intval', $this->getConnection()->fetchCol($select));
    }

    /**
     * @param int[] $productIds
     * @return int[]
     */
    public function retrieveVariantsForParents(array $productIds, int $storeId): array
    {
        /** The base query (for community DB version) is:
         * SELECT DISTINCT entity.entity_id AS childId
         *   FROM catalog_product_entity entity
         *   JOIN catalog_product_relation relation ON relation.child_id = entity.entity_id
         *   JOIN catalog_product_entity parent ON parent.entity_id = relation.parent_id
         *  WHERE parent.type_id = 'configurable'
         *    AND relation.parent_id IN ($productIds)
         *  ORDER BY entity.entity_id
        */
        $linkFieldId = $this->productMetaData->getLinkField();
        $productIdField = $this->productMetaData->getIdentifierField();
        $entityTable = $this->resourceConnection->getTableName($this->productMetaData->getEntityTable());
        $relationTable = $this->resourceConnection->getTableName('catalog_product_relation');
        $productIdsString = implode(',', $productIds);

        $select = $this->getConnection()->select()
            ->from(['entity' => $entityTable], $productIdField)
            ->join(['relation' => $relationTable], "relation.child_id = entity.$linkFieldId", [])
            ->join(['parent' => $entityTable], "parent.$linkFieldId = relation.parent_id", [])
            ->where("parent.type_id = 'configurable'")
            ->where("relation.parent_id IN($productIdsString)");

        $this->websiteAndStatusAndVisibleSelectModifier->modifyAll($select, $storeId);

        return array_map('intval', $this->getConnection()->fetchCol($select));
    }

    /**
     * Get list of attribute ids used to create configurable products
     */
    public function getConfigurableAttributeIds(): array
    {
        if (null === $this->configurableAttributeIds) {
            $select = $this->getConnection()->select();
            $select->from(
                $this->resourceConnection->getTableName('catalog_product_super_attribute'),
                ['attribute_id']
            );

            $select->distinct();

            $this->configurableAttributeIds = $this->getConnection()->fetchCol($select);
        }

        return $this->configurableAttributeIds;
    }

    private function getConnection(): AdapterInterface
    {
        return $this->resourceConnection->getConnection();
    }
}
