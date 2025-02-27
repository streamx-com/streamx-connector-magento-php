<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel\Product;

use Exception;
use StreamX\ConnectorCatalog\Model\ProductMetaData;
use Magento\Catalog\Helper\Data;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Select;
use Magento\Store\Model\StoreManagerInterface;

class Bundle
{
    private ResourceConnection $resource;
    private ?array $products;
    private ?array $bundleProductIds;
    private array $bundleOptionsByProduct = [];
    private ProductMetaData $productMetaData;
    private StoreManagerInterface $storeManager;
    private Data $catalogHelper;

    public function __construct(
        ProductMetaData $productMetaData,
        ResourceConnection $resourceModel,
        StoreManagerInterface $storeManager,
        Data $catalogHelper
    ) {
        $this->resource = $resourceModel;
        $this->productMetaData = $productMetaData;
        $this->storeManager = $storeManager;
        $this->catalogHelper = $catalogHelper;
    }

    /**
     * @throws Exception
     */
    public function setProducts(array $products): void
    {
        $linkField = $this->productMetaData->getLinkField();

        foreach ($products as $product) {
            $this->products[$product[$linkField]] = $product;
        }
    }

    /**
     * Clear data
     */
    public function clear(): void
    {
        $this->products = null;
        $this->bundleOptionsByProduct = [];
        $this->bundleProductIds = null;
    }

    public function loadBundleOptions(int $storeId): array
    {
        $productIds = $this->getBundleIds();

        if (empty($productIds)) {
            return [];
        }

        $this->initOptions($storeId);
        $this->initSelection($storeId);

        return $this->bundleOptionsByProduct;
    }

    /**
     * Init Options
     */
    private function initOptions(int $storeId): void
    {
        $bundleOptions = $this->getBundleOptionsFromResource($storeId);

        foreach ($bundleOptions as $bundleOption) {
            /* entity_id or row_id*/
            $parentId = $bundleOption['parent_id'];
            $parentEntityId = $this->products[$parentId]['entity_id'];
            $optionId = $bundleOption['option_id'];

            $this->bundleOptionsByProduct[$parentEntityId][$optionId] = [
                'option_id' => (int)($bundleOption['option_id']),
                'position' => (int)($bundleOption['position']),
                'type' => $bundleOption['type'],
                'sku' => $this->products[$parentId]['sku'],
                'title' => $bundleOption['title'],
                'required' => (bool)$bundleOption['required'],
            ];
        }
    }

    /**
     * Append Selection
     */
    private function initSelection(int $storeId): void
    {
        $bundleSelections = $this->getBundleSelections($storeId);
        $simpleIds = array_column($bundleSelections, 'product_id');
        $simpleSkuList = $this->getProductSku($simpleIds);

        foreach ($bundleSelections as $selection) {
            $optionId = $selection['option_id'];
            /*row_id or entity_id*/
            $parentId = $selection['parent_product_id'];
            $entityId = $this->products[$parentId]['entity_id'];
            $productId = $selection['product_id'];
            $bundlePriceType = $this->products[$parentId]['price_type'] ?? null; // TODO: price_type should probably never be null

            $selectionPriceType = $bundlePriceType ? $selection['selection_price_type'] : null;
            $selectionPrice = $bundlePriceType ? $selection['selection_price_value'] : null;

            $this->bundleOptionsByProduct[$entityId][$optionId]['product_links'][] = [
                'id' => (int)$selection['selection_id'],
                'is_default' => (bool)$selection['is_default'],
                'qty' => (float)$selection['selection_qty'],
                'can_change_quantity' => (bool)$selection['selection_can_change_qty'],
                'price' => (float)$selectionPrice,
                'price_type' => $selectionPriceType,
                'position' => (int)($selection['position']),
                'sku' => $simpleSkuList[$productId],
            ];
        }
    }

    private function getBundleSelections($storeId): array
    {
        $productIds = $this->getBundleIds();
        $connection = $this->getConnection();

        $select = $connection->select()->from(
            ['selection' => $this->resource->getTableName('catalog_product_bundle_selection')]
        );
        $productIdColumn = 'parent_product_id';

        if (!$this->catalogHelper->isPriceGlobal()) {
            $websiteId = $this->storeManager->getStore($storeId)
                ->getWebsiteId();
            $priceType = $connection->getCheckSql(
                'price.selection_price_type IS NOT NULL',
                'price.selection_price_type',
                'selection.selection_price_type'
            );
            $priceValue = $connection->getCheckSql(
                'price.selection_price_value IS NOT NULL',
                'price.selection_price_value',
                'selection.selection_price_value'
            );
            $select->joinLeft(
                ['price' => $this->resource->getTableName('catalog_product_bundle_selection_price')],
                "selection.selection_id = price.selection_id AND price.website_id = $websiteId" .
                ' AND selection.parent_product_id = price.parent_product_id',
                [
                    'selection_price_type' => $priceType,
                    'selection_price_value' => $priceValue,
                    'parent_product_id' => 'selection.parent_product_id'
                ]
            );
            $productIdColumn = 'selection.' . $productIdColumn;
        }

        $select->where("$productIdColumn IN (?)", $productIds);

        return $this->getConnection()->fetchAll($select);
    }

    private function getProductSku(array $productIds): array
    {
        $select = $this->getConnection()->select();
        $select->from($this->resource->getTableName('catalog_product_entity'), ['entity_id', 'sku']);
        $select->where('entity_id IN (?)', $productIds);

        return $this->getConnection()->fetchPairs($select);
    }

    private function getBundleOptionsFromResource(int $storeId): array
    {
        $productIds = $this->getBundleIds();

        $select = $this->getConnection()->select()->from(
            ['main_table' => $this->resource->getTableName('catalog_product_bundle_option')]
        );

        $select->where('parent_id IN (?)', $productIds);
        $select->order('main_table.position asc')
            ->order('main_table.option_id asc');

        $this->joinOptionValues($select, $storeId);

        return $this->getConnection()->fetchAll($select);
    }

    private function joinOptionValues(Select $select, int $storeId): void
    {
        $select
            ->joinLeft(
                ['option_value_default' => $this->resource->getTableName('catalog_product_bundle_option_value')],
                'main_table.option_id = option_value_default.option_id and option_value_default.store_id = 0',
                []
            )
            ->columns(['default_title' => 'option_value_default.title']);

        $title = $this->getConnection()->getCheckSql(
            'option_value.title IS NOT NULL',
            'option_value.title',
            'option_value_default.title'
        );

        $select->columns(['title' => $title])
            ->joinLeft(
                ['option_value' => $this->resource->getTableName('catalog_product_bundle_option_value')],
                $this->getConnection()->quoteInto(
                    'main_table.option_id = option_value.option_id and option_value.store_id = ?',
                    $storeId
                ),
                []
            );
    }

    /**
     * @throws Exception
     */
    private function getBundleIds(): array
    {
        if (null === $this->bundleProductIds) {
            $this->bundleProductIds = [];

            foreach ($this->products as $productData) {
                if ('bundle' === $productData['type_id']) {
                    $linkFieldId = $this->productMetaData->getLinkField();
                    $this->bundleProductIds[] = $productData[$linkFieldId];
                }
            }
        }

        return $this->bundleProductIds;
    }

    private function getConnection(): AdapterInterface
    {
        return $this->resource->getConnection();
    }
}
