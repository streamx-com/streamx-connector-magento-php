<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel;

use Exception;
use Magento\Framework\DB\Adapter\AdapterInterface;
use StreamX\ConnectorCatalog\Model\ResourceModel\Product\EligibleProductSelectModifier;
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
    private EligibleProductSelectModifier $eligibleProductSelectModifier;

    public function __construct(
        CatalogConfig $configSettings,
        EligibleProductSelectModifier $eligibleProductSelectModifier,
        ResourceConnection $resourceConnection,
        ProductMetaData $productMetaData,
        DbHelper $dbHelper
    ) {
        $this->resourceConnection = $resourceConnection;
        $this->dbHelper = $dbHelper;
        $this->productSettings = $configSettings;
        $this->eligibleProductSelectModifier = $eligibleProductSelectModifier;
        $this->productMetaData = $productMetaData;
    }

    /**
     * @throws Exception
     */
    public function getProducts(int $storeId, array $productIds, int $fromId, int $limit = 1000): array
    {
        $select = $this
            ->prepareProductSelect($this->getRequiredColumns(), $storeId)
            ->where("entity.entity_id > ?", $fromId)
            ->limit($limit);

        if (!empty($productIds)) {
            $select->where("entity.entity_id IN (?)", $productIds);
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
        $select = $this->prepareBaseProductSelect($columns, $storeId, true);
        $this->addProductTypeFilter($select);
        $select->order('entity.entity_id ASC');
        return $select;
    }

    /**
     * Prepares base product select for selecting main or variant products
     */
    private function prepareBaseProductSelect(array $requiredColumns, int $storeId, bool $filterByVisibility): Select
    {
        $select = $this->getConnection()->select()
            ->from(
                ['entity' => $this->productMetaData->getEntityTable()],
                $requiredColumns
            );

        $this->eligibleProductSelectModifier->modify($select, $storeId, $filterByVisibility);

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
        $entityIdField = $this->productMetaData->getIdentifierField();
        $columns = [
            'sku',
            $entityIdField,
        ];

        if ($linkField !== $entityIdField) {
            $columns[] = $linkField;
        }

        // when loading children (variants) for a configurable product, don't filter by visibility
        $select = $this->prepareBaseProductSelect($columns, $storeId, false);

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
     * @param int $storeId
     * @return int[]
     */
    public function loadIdsOfProductsThatUseAttributes(array $attributeIds, int $storeId): array {
        return $this->internalLoadIdsOfProductsThatUseAttributes($attributeIds, $storeId, false);
    }

    /**
     * @param array $attributeIds
     * @param int $storeId
     * @return int[]
     */
    public function loadIdsOfChildProductsThatUseAttributes(array $attributeIds, int $storeId): array {
        return $this->internalLoadIdsOfProductsThatUseAttributes($attributeIds, $storeId, true);
    }

    /**
     * @param int[] $attributeIds
     * @param bool $loadVariants if true, returns only IDs of variant (child) products. If false - returns only IDs that are not variant (child) products
     * @return int[]
     * @throws Zend_Db_Select_Exception
     */
    private function internalLoadIdsOfProductsThatUseAttributes(array $attributeIds, int $storeId, bool $loadVariants): array {
        if (empty($attributeIds)) {
            return [];
        }

        $linkField = $this->productMetaData->getLinkField();
        $connection = $this->getConnection();

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
            // filter by is product a variant or not:
            ->joinLeft(['relation' => 'catalog_product_relation'], 'relation.child_id = entity.entity_id', [])
            ->joinLeft(['parent' => 'catalog_product_entity'], 'relation.parent_id = parent.entity_id', ['parent.type_id'])
            ->where($loadVariants
                ? "parent.type_id = 'configurable'"
                : "parent.type_id is null OR parent.type_id <> 'configurable'")
            ->order('entity_id');

        $this->eligibleProductSelectModifier->modify($selectProductEntityIds, $storeId, true);

        return $connection->fetchCol($selectProductEntityIds);
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
