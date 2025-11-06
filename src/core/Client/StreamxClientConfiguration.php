<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client;

use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\App\ResourceConnection;
use StreamX\ConnectorCore\Api\BaseConfigurationReader;

class StreamxClientConfiguration extends BaseConfigurationReader
{
    public function __construct(ResourceConnection $connection, ScopeConfigInterface $scopeConfig) {
        parent::__construct($connection, $scopeConfig, 'streamx_client');
    }

    public function getIngestionBaseUrl(int $storeId): string {
        return (string)$this->getStoreLevelConfigValue('ingestion_base_url', $storeId);
    }

    public function getEventSource(int $storeId): string {
        return (string)$this->getStoreLevelConfigValue('event_source', $storeId);
    }

    public function getPublishingEventType(int $storeId): string {
        return (string)$this->getStoreLevelConfigValue('publishing_event_type', $storeId);
    }

    public function getUnpublishingEventType(int $storeId): string {
        return (string)$this->getStoreLevelConfigValue('unpublishing_event_type', $storeId);
    }

    public function getAuthToken(int $storeId): ?string {
        return $this->getStoreLevelConfigValue('auth_token', $storeId);
    }

    public function getConnectionTimeout(int $storeId): int {
        return (int)$this->getStoreLevelConfigValue('connection_timeout', $storeId);
    }

    public function getResponseTimeout(int $storeId): int {
        return (int)$this->getStoreLevelConfigValue('response_timeout', $storeId);
    }

    public function shouldDisableCertificateValidation(int $storeId): bool {
        return (bool)$this->getStoreLevelConfigValue('disable_certificate_validation', $storeId);
    }
}
