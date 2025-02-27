<?php declare(strict_types = 1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel\Product;

use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Exception\NoSuchEntityException;
use Magento\Store\Api\Data\StoreInterface;
use Magento\Store\Model\StoreManagerInterface;
use Magento\CatalogRule\Model\ResourceModel\Rule\Product\Price as CatalogRulePrice;

use StreamX\ConnectorCatalog\Model\SystemConfig\CatalogConfig;
use StreamX\ConnectorCatalog\Model\ProductMetaData;
use StreamX\ConnectorCatalog\Model\Product\PriceTableResolverProxy;

class Prices
{
    private ResourceConnection $resource;
    private CatalogConfig $settings;
    private StoreManagerInterface $storeManager;
    private ProductMetaData $productMetaData;
    private PriceTableResolverProxy $priceTableResolver;
    private CatalogRulePrice $catalogPriceResourceModel;

    public function __construct(
        ResourceConnection $resourceModel,
        StoreManagerInterface $storeManager,
        ProductMetaData $productMetaData,
        CatalogConfig $catalogSettings,
        CatalogRulePrice $catalogPriceResourceModel,
        PriceTableResolverProxy $priceTableResolver
    ) {
        $this->resource = $resourceModel;
        $this->storeManager = $storeManager;
        $this->productMetaData = $productMetaData;
        $this->priceTableResolver = $priceTableResolver;
        $this->settings = $catalogSettings;
        $this->catalogPriceResourceModel = $catalogPriceResourceModel;
    }

    /**
     * Only default customer Group ID (0) is supported now
     *
     * @throws NoSuchEntityException
     */
    public function loadPriceDataFromPriceIndex(int $storeId, array $productIds): array
    {
        $entityIdField = $this->productMetaData->getIdentifierField();
        $websiteId = (int)$this->getStore($storeId)->getWebsiteId();

        // Only default customer Group ID (0) is supported now
        $customerGroupId = 0;
        $priceIndexTableName = $this->getPriceIndexTableName($websiteId, $customerGroupId);

        $select = $this->getConnection()->select()
            ->from(
                ['p' => $priceIndexTableName],
                [
                    $entityIdField,
                    'price',
                    'final_price',
                ]
            )
            ->where('p.customer_group_id = ?', $customerGroupId)
            ->where('p.website_id = ?', $websiteId)
            ->where("p.$entityIdField IN (?)", $productIds);

        $prices = $this->getConnection()->fetchAssoc($select);

        if ($this->settings->useCatalogRules()) {
            $catalogPrices = $this->getCatalogRulePrices($websiteId, $productIds);

            foreach ($catalogPrices as $productId => $finalPrice) {
                $priceIndexerPrice = $prices[$productId]['final_price'] ?? $finalPrice;
                $prices[$productId]['final_price'] = min($finalPrice, $priceIndexerPrice);
            }
        }

        return $prices;
    }

    private function getCatalogRulePrices(int $websiteId, array $productsIds): array
    {
        $connection = $this->getConnection();
        $select = $connection->select();
        $select->join(
            ['cpiw' => $this->catalogPriceResourceModel->getTable('catalog_product_index_website')],
            'cpiw.website_id = i.website_id',
            []
        );
        $select->join(
            ['cpp' => $this->catalogPriceResourceModel->getMainTable()],
            'cpp.website_id = cpiw.website_id'
            . ' AND cpp.rule_date = cpiw.website_date',
            []
        );

        // Only default customer Group ID (0) is supported now
        $customerGroupId = 0;
        $select->where('cpp.product_id IN (?)', $productsIds);
        $select->where('cpp.customer_group_id = ?', $customerGroupId);
        $select->where('cpp.website_id = ?', $websiteId);
        $select->columns([
            'product_id' => 'cpp.product_id',
            'final_price' => 'cpp.rule_price',
        ]);

        return $connection->fetchPairs($select);
    }

    private function getPriceIndexTableName(int $websiteId, int $customerGroupId): string
    {
        return $this->priceTableResolver->resolve($websiteId, $customerGroupId);
    }

    /**
     * @throws NoSuchEntityException
     */
    private function getStore(int $storeId): StoreInterface
    {
        return $this->storeManager->getStore($storeId);
    }

    private function getConnection(): AdapterInterface
    {
        return $this->resource->getConnection();
    }
}
