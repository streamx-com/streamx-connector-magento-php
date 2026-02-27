<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Indexer;

use Magento\Framework\UrlInterface;
use Magento\Store\Model\StoreManagerInterface;
use StreamX\ConnectorCatalog\Model\SystemConfig\CatalogConfig;

class ImageUrlManager {

    private StoreManagerInterface $storeManager;
    private CatalogConfig $settings;

    public function __construct(StoreManagerInterface $storeManager, CatalogConfig $settings) {
        $this->storeManager = $storeManager;
        $this->settings = $settings;
    }

    public function getProductImageUrl(string $imageRelativePath, int $storeId): string {
        $store = $this->storeManager->getStore($storeId);
        $relativeUrl = '/media/catalog/product/' . ltrim($imageRelativePath, '/');

        if ($this->settings->useRelativeUrlsForImages()) {
            return $relativeUrl;
        }

        $storeBaseUrl = $store->getBaseUrl(UrlInterface::URL_TYPE_LINK, $this->settings->useRelativeUrlsForImages());
        return rtrim($storeBaseUrl, '/') . $relativeUrl;
    }
}
