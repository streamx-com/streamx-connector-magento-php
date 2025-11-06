<?php

namespace StreamX\ConnectorTestEndpoints\Impl;

use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Framework\App\Config\Storage\WriterInterface;
use Magento\Store\Api\Data\WebsiteInterface;
use Magento\Store\Api\GroupRepositoryInterface;
use Magento\Store\Api\StoreRepositoryInterface;
use Magento\Store\Api\WebsiteRepositoryInterface;
use Magento\Store\Model\Group;
use Magento\Store\Model\ScopeInterface;
use Magento\Store\Model\Store;
use Magento\Store\Model\StoreManagerInterface;
use Magento\Store\Model\Website;
use Magento\Store\Model\WebsiteFactory;
use Magento\Store\Model\GroupFactory;
use Magento\Store\Model\StoreFactory;
use StreamX\ConnectorTestEndpoints\Api\StoresControllerInterface;

class StoresControllerImpl implements StoresControllerInterface {

    private const STORE_2_CODE = 'store_2';
    private const SECOND_WEBSITE_CODE = 'second_website';
    private const STORE_CODE_FOR_SECOND_WEBSITE = 'store_for_second_website';

    private const PRODUCT_IDS_IN_SECOND_WEBSITE = [4, 5, 6, 61, 62]; // 4, 5, 6 are simple products. 61 is a variant of configurable product 62

    private const CONNECTOR_ENABLE_CONFIG_KEY = 'streamx_connector_settings/general_settings/enable';
    private const RABBIT_MQ_ENABLE_CONFIG_KEY = 'streamx_connector_settings/rabbit_mq/enable';
    private const INDEXED_STORES_CONFIG_KEY = 'streamx_connector_settings/general_settings/indexed_stores';

    private WebsiteFactory $websiteFactory;
    private GroupFactory $groupFactory;
    private StoreFactory $storeFactory;
    private WebsiteRepositoryInterface $websiteRepository;
    private GroupRepositoryInterface $groupRepository;
    private StoreRepositoryInterface $storeRepository;
    private StoreManagerInterface $storeManager;
    private WriterInterface $writer;
    private ProductRepositoryInterface $productRepository;

    public function __construct(
        WebsiteFactory $websiteFactory,
        GroupFactory $groupFactory,
        StoreFactory $storeFactory,
        WebsiteRepositoryInterface $websiteRepository,
        GroupRepositoryInterface $groupRepository,
        StoreRepositoryInterface $storeRepository,
        StoreManagerInterface $storeManager,
        WriterInterface $writer,
        ProductRepositoryInterface $productRepository
    ) {
        $this->websiteFactory = $websiteFactory;
        $this->groupFactory = $groupFactory;
        $this->storeFactory = $storeFactory;
        $this->websiteRepository = $websiteRepository;
        $this->groupRepository = $groupRepository;
        $this->storeRepository = $storeRepository;
        $this->storeManager = $storeManager;
        $this->writer = $writer;
        $this->productRepository = $productRepository;
    }

    /**
     * @inheritdoc
     */
    public function setUpStoresAndWebsites(): string {
        // make sure StreamX Connector is turned on
        $this->setGlobalLevelConfigValue(self::CONNECTOR_ENABLE_CONFIG_KEY, 1);

        // make sure RabbitMQ is enabled.
        $this->setGlobalLevelConfigValue(self::RABBIT_MQ_ENABLE_CONFIG_KEY, 1);

        $defaultWebsite = array_values($this->storeManager->getWebsites())[0];
        $defaultStoreId = $this->storeManager->getStore('default')->getId();

        $store2 = $this->getOrCreateStore($defaultWebsite->getId(), self::STORE_2_CODE, self::STORE_2_CODE);
        $secondWebsite = $this->getOrCreateWebsite(self::SECOND_WEBSITE_CODE);
        $storeForSecondWebsite = $this->getOrCreateStore($secondWebsite->getId(), self::STORE_CODE_FOR_SECOND_WEBSITE, self::STORE_CODE_FOR_SECOND_WEBSITE);

        // configure exported stores
        $this->setIndexedStoresForWebsite($defaultWebsite, $defaultStoreId, $store2->getId());
        $this->setIndexedStoresForWebsite($secondWebsite, $storeForSecondWebsite->getId());

        // add products to the new website
        $secondWebsiteId = $secondWebsite->getId();
        foreach (self::PRODUCT_IDS_IN_SECOND_WEBSITE as $productId) {
            $product = $this->productRepository->getById($productId);
            $websiteIds = array_unique(array_merge($product->getWebsiteIds(), [$secondWebsiteId]));
            $product->setWebsiteIds($websiteIds);
            $this->productRepository->save($product);
        }

        return 'OK';
    }

    private function getOrCreateStore(int $websiteId, string $storeCode, string $viewCode): Store {
        foreach ($this->storeRepository->getList() as $store) {
            if ($store->getCode() == $storeCode) {
                return $this->storeFactory->create()->load($store->getId());
            }
        }

        $storeGroup = $this->getOrCreateStoreGroup($websiteId, $storeCode);

        $storeView = $this->storeFactory->create()
            ->setCode($viewCode)
            ->setWebsiteId($websiteId)
            ->setStoreGroupId($storeGroup->getId())
            ->setName(self::codeToName($viewCode))
            ->setIsActive(true)
            ->save();

        $storeGroup->setDefaultStoreId($storeView->getId())->save();

        return $storeView;
    }

    private function getOrCreateStoreGroup(int $websiteId, string $code): Group {
        foreach ($this->groupRepository->getList() as $group) {
            if ($group->getCode() == $code) {
                return $this->groupFactory->create()->load($group->getId());
            }
        }

        return $this->groupFactory->create()
            ->setWebsiteId($websiteId)
            ->setCode($code)
            ->setName(self::codeToName($code))
            ->setRootCategoryId(2)
            ->save();
    }

    private function getOrCreateWebsite(string $code): Website {
        foreach ($this->websiteRepository->getList() as $website) {
            if ($website->getCode() == $code) {
                return $this->websiteFactory->create()->load($website->getId());
            }
        }

        return $this->websiteFactory->create()
            ->setCode($code)
            ->setName(self::codeToName($code))
            ->save();
    }

    private static function codeToName(string $code): string {
        return strtoupper($code);
    }

    private function setIndexedStoresForWebsite(WebsiteInterface $website, string... $storeIds): void {
        $this->writer->save(
            self::INDEXED_STORES_CONFIG_KEY,
            implode(',', $storeIds),
            ScopeInterface::SCOPE_WEBSITES,
            $website->getId()
        );
    }

    private function setGlobalLevelConfigValue(string $key, string $value): void {
        $this->writer->save($key, $value);
    }
}
