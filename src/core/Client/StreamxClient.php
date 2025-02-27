<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client;

use Exception;
use GuzzleHttp\Client as GuzzleHttpClient;
use Magento\Store\Api\Data\StoreInterface;
use Psr\Log\LoggerInterface;
use Streamx\Clients\Ingestion\Builders\StreamxClientBuilders;
use Streamx\Clients\Ingestion\Publisher\Message;
use Streamx\Clients\Ingestion\Publisher\Publisher;
use StreamX\ConnectorCatalog\Model\Indexer\CategoryProcessor;
use StreamX\ConnectorCatalog\Model\Indexer\ProductProcessor;
use StreamX\ConnectorCore\Client\Model\Data;

class StreamxClient {

    private const STREAMX_TYPE_PROPERTY_NAME = 'streamx:type';

    private LoggerInterface $logger;
    private string $storeCode;
    private Publisher $schemasFetcher;
    private Publisher $dataIngestor;

    public function __construct(
        LoggerInterface $logger,
        StreamxClientConfiguration $configuration,
        StoreInterface $store
    ) {
        $this->logger = $logger;
        $this->storeCode = $store->getCode();
        $storeId = (int) $store->getId();
        $this->schemasFetcher = $this->createStreamxPublisher($configuration, $storeId, false);
        $this->dataIngestor = $this->createStreamxPublisher($configuration, $storeId, true);
    }

    public function publish(array $entities, string $indexerName): void {
        $publishMessages = [];
        foreach ($entities as $entity) {
            $entityType = EntityType::fromEntityAndIndexerName($entity, $indexerName);
            $key = $this->createStreamxKey($entityType, $entity['id']);
            $payload = new Data(json_encode($entity));
            $publishMessages[] = Message::newPublishMessage($key, $payload)
                ->withProperty(self::STREAMX_TYPE_PROPERTY_NAME, $entityType->getFullyQualifiedName())
                ->build();
        }

        $this->ingest($publishMessages, "publishing", $indexerName);
    }

    public function unpublish(array $entityIds, string $indexerName): void {
        $unpublishMessages = [];
        foreach ($entityIds as $entityId) {
            $entityType = EntityType::fromIndexerName($indexerName);
            $key = $this->createStreamxKey($entityType, $entityId);
            $unpublishMessages[] = Message::newUnpublishMessage($key)
                ->withProperty(self::STREAMX_TYPE_PROPERTY_NAME, $entityType->getFullyQualifiedName())
                ->build();
        }

        $this->ingest($unpublishMessages, "unpublishing", $indexerName);
    }

    private function createStreamxKey(EntityType $entityType, int $entityId): string {
        return sprintf('%s_%s:%d',
            $this->storeCode,
            $entityType->getRootType(),
            $entityId
        );
    }

    /**
     * @param Message[] $ingestionMessages
     */
    private function ingest(array $ingestionMessages, string $operationName, string $indexerName): void {
        $keys = array_column($ingestionMessages, 'key');
        $messagesCount = count($ingestionMessages);
        $this->logger->info("Start $operationName $messagesCount entities from $indexerName with keys " . json_encode($keys));

        try {
            // TODO make sure this will never block. Best by turning off Pulsar container
            $messageStatuses = $this->dataIngestor->sendMulti($ingestionMessages);

            foreach ($messageStatuses as $messageStatus) {
                if ($messageStatus->getSuccess() === null) {
                    $this->logger->error('Ingestion failure: ' . json_encode($messageStatus->getFailure()));
                }
            }
        } catch (Exception $e) {
            $this->logException('Ingestion exception', $e);
        }

        $this->logger->info("Finished $operationName $messagesCount entities from $indexerName");
    }

    public function isStreamxAvailable(): bool {
        try {
            $schema = $this->schemasFetcher->fetchSchema();
            if (str_contains($schema, 'IngestionMessage')) {
                return true;
            }
            $this->logger->error("Requested StreamX channel is not available, Ingestion Message definition is missing in schema:\n$schema");
        } catch (Exception $e) {
            $this->logException('Exception checking if StreamX is available', $e);
        }
        return false;
    }

    private function createStreamxPublisher(StreamxClientConfiguration $configuration, int $storeId, bool $stream): Publisher {
        $httpClient = new GuzzleHttpClient([
            'connect_timeout' => 1, // maximum time (in seconds) to establish the connection
            'timeout' => 5, // maximum time (in seconds) to wait for response
            'verify' => !$configuration->shouldDisableCertificateValidation($storeId),
            'stream' => $stream
        ]);

        $ingestionClientBuilder = StreamxClientBuilders::create($configuration->getIngestionBaseUrl($storeId))
            ->setHttpClient($httpClient);

        if ($configuration->getAuthToken($storeId)) {
            $ingestionClientBuilder->setAuthToken($configuration->getAuthToken($storeId));
        }

        return $ingestionClientBuilder->build()->newPublisher(
            $configuration->getChannelName($storeId),
            $configuration->getChannelSchemaName($storeId)
        );
    }

    private function logException(string $customMessage, Exception $e): void {
        $this->logger->error(
            $customMessage . ': ' . $e->getMessage(),
            [
                'Exception' => $e,
                'Stack trace' => $e->getTraceAsString(),
            ]
        );
    }
}
