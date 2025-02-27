<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel;

use Exception;
use Magento\Framework\DB\Adapter\AdapterInterface;
use StreamX\ConnectorCatalog\Model\CategoryMetaData;
use StreamX\ConnectorCatalog\Model\ResourceModel\Category\ActiveCategorySelectModifier;
use StreamX\ConnectorCatalog\Model\ResourceModel\Category\CategoryFromStoreSelectModifier;
use Magento\Framework\App\ResourceConnection;
use Magento\Catalog\Model\Category as CoreCategoryModel;
use Magento\Framework\DB\Select;
use Zend_Db_Select;

class Category
{
    private ResourceConnection $resource;
    private CompositeSelectModifier $selectModifier;
    private CategoryMetaData $categoryMetaData;

    public function __construct(
        CategoryFromStoreSelectModifier $categoryFromStoreSelectModifier,
        ActiveCategorySelectModifier $activeCategorySelectModifier,
        ResourceConnection $resourceConnection,
        CategoryMetaData $categoryMetaData
    ) {
        $this->resource = $resourceConnection;
        $this->categoryMetaData = $categoryMetaData;
        $this->selectModifier = new CompositeSelectModifier($categoryFromStoreSelectModifier, $activeCategorySelectModifier);
    }

    /**
     * @throws Exception
     */
    public function getCategories(int $storeId, array $categoryIds = [], int $fromId = 0, int $limit = 1000): array
    {
        $select = self::getCategoriesBaseSelect($this->resource, $this->categoryMetaData);
        $this->selectModifier->modifyAll($select, $storeId);

        if (!empty($categoryIds)) {
            $select->where("entity.entity_id IN (?)", $categoryIds);
        }

        $select->where("entity.entity_id > ?", $fromId);
        $select->limit($limit);
        $select->order("entity.entity_id ASC");

        return $this->getConnection()->fetchAll($select);
    }

    /**
     * @return array<int, array<int>> key = product id, value = array of the product's category ids
     * @throws Exception
     */
    public function getProductCategoriesMap(int $storeId, array $productIds): array
    {
        $select = $this->getConnection()->select()->from(
            ['entity' => $this->categoryMetaData->getEntityTable()]
        );

        $this->selectModifier->modifyAll($select, $storeId);
        $table = $this->resource->getTableName('catalog_category_product');
        $entityIdField = $this->categoryMetaData->getEntityIdField();
        $select->reset(Zend_Db_Select::COLUMNS);
        $select->joinInner(
            ['cpi' => $table],
            "entity.$entityIdField = cpi.category_id",
            [
                'category_id',
                'product_id'
            ]
        )->where('cpi.product_id IN (?)', $productIds);

        $rows = $this->getConnection()->fetchAll($select);
        return $this->toProductCategoriesMap($rows);
    }

    private function toProductCategoriesMap(array $rows): array
    {
        $result = [];
        foreach ($rows as $row) {
            $categoryId = (int) $row['category_id'];
            $productId = (int) $row['product_id'];
            $result[$productId][] = $categoryId;
        }
        return $result;
    }

    /**
     * @throws Exception
     */
    public function getParentIds(array $categoryIds): array
    {
        $entityField = $this->categoryMetaData->getEntityIdField();

        $select = $this->getConnection()->select()->from(
            ['entity' => $this->categoryMetaData->getEntityTable()],
            ['path']
        );

        $select->where(
            "$entityField IN (?)",
            array_map('intval', $categoryIds)
        );

        $paths = $this->getConnection()->fetchCol($select);
        $parentIds = [];

        foreach ($paths as $path) {
            $path = explode('/', $path);

            foreach ($path as $catId) {
                $catId = (int)$catId;

                if ($catId !== CoreCategoryModel::TREE_ROOT_ID) {
                    $parentIds[] = $catId;
                }
            }
        }

        return array_unique($parentIds);
    }

    /**
     * @return int[]
     * @throws Exception
     */
    public function getAllSubCategories(int $categoryId): array
    {
        $entityField = $this->categoryMetaData->getEntityIdField();
        $connection = $this->getConnection();
        $select = $connection->select()->from(
            ['entity' => $this->categoryMetaData->getEntityTable()],
            [$entityField]
        );

        $catIdExpr = $connection->quote("%/$categoryId/%");
        $select->where("path like $catIdExpr");

        return $connection->fetchCol($select);
    }

    private function getConnection(): AdapterInterface
    {
        return $this->resource->getConnection();
    }

    public static function getCategoriesBaseSelect(ResourceConnection $resource, CategoryMetaData $metaData): Select
    {
        $linkField = $metaData->getLinkField();

        return $resource->getConnection()
            ->select()
            ->from(
                ['entity' => $metaData->getEntityTable()], // alias for the catalog_category_entity table, to use in joins
                ['parent_id', 'path'] // columns to select
            )->columns( // select also entity_id column, but alias it to id
                ['id' => 'entity_id']
            )->joinLeft( // join eav_entity_type table to read entity_type_id
                ['e' => $resource->getTableName('eav_entity_type')],
                "e.entity_table = '{$metaData->getEntityTable()}'",
                [] // don't include any columns in the query results
            )->joinLeft( // join eav_attribute table to read category name attribute definition
                ['name_attr' => $resource->getTableName('eav_attribute')],
                "name_attr.attribute_code = 'name' AND name_attr.entity_type_id = e.entity_type_id",
                [] // don't include any columns in the query results
            )->joinLeft( // join eav_attribute table to read category's url_key definition
                ['url_key_attr' => $resource->getTableName('eav_attribute')],
                "url_key_attr.attribute_code = 'url_key' AND url_key_attr.entity_type_id = e.entity_type_id",
                [] // don't include any columns in the query results
            )->joinLeft( // join catalog_category_entity_varchar table to read actual category name
                ['category_name_attr' => $resource->getTableName('catalog_category_entity_varchar')],
                "category_name_attr.$linkField = entity.$linkField AND category_name_attr.attribute_id = name_attr.attribute_id",
                ['name' => 'value'] // include attr value as "name" in the query results
            )->joinLeft( // join catalog_category_entity_varchar table to read actual category url_key
                ['category_url_key_attr' => $resource->getTableName('catalog_category_entity_varchar')],
                "category_url_key_attr.$linkField = entity.$linkField AND category_url_key_attr.attribute_id = url_key_attr.attribute_id",
                ['url_key' => 'value'] // include attr value as "url_key" in the query results
            );
    }
}
