<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\SystemConfig;

use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\App\ResourceConnection;
use StreamX\ConnectorCore\Api\BaseConfigurationReader;

class CatalogConfig extends BaseConfigurationReader
{
    public function __construct(ResourceConnection $connection, ScopeConfigInterface $scopeConfig) {
        parent::__construct($connection, $scopeConfig, 'catalog_settings');
    }

    public function slugGenerationStrategy(): int {
        return (int)$this->getGlobalConfigValue('slug_generation_strategy');
    }

    public function usePricesIndex(): bool {
        return (bool)$this->getGlobalConfigValue('use_prices_index');
    }

    public function useCatalogPriceRules(): bool {
        return (bool)$this->getGlobalConfigValue('use_catalog_price_rules');
    }

    public function useRelativeUrlsForImages(): bool {
        return (bool)$this->getGlobalConfigValue('use_relative_urls_for_images');
    }

    public function shouldExportProductsNotVisibleIndividually(): bool {
        return (bool)$this->getGlobalConfigValue('export_products_not_visible_individually');
    }

    public function getAllowedProductTypes(): array {
        return parent::splitCommaSeparatedValueToArray(
            $this->getGlobalConfigValue('allowed_product_types')
        );
    }

    public function getProductAttributesToIndex(int $storeId): array {
        return parent::splitCommaSeparatedValueToArray(
            $this->getStoreLevelConfigValue('product_attributes', $storeId)
        );
    }

    public function getChildProductAttributesToIndex(int $storeId): array {
        return parent::splitCommaSeparatedValueToArray(
            $this->getStoreLevelConfigValue('child_product_attributes', $storeId)
        );
    }
}
