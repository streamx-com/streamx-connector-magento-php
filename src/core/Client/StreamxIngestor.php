<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client;

use CloudEvents\V1\CloudEventInterface;
use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\RequestOptions;
use Psr\Log\LoggerInterface;
use Streamx\Clients\Ingestion\Builders\StreamxClientBuilders;
use Streamx\Clients\Ingestion\Publisher\Publisher;
use StreamX\ConnectorCore\Client\Model\CloudEventUtils;
use StreamX\ConnectorCore\Traits\ExceptionLogger;

class StreamxIngestor {
    use ExceptionLogger;

    private LoggerInterface $logger;
    private StreamxClientConfiguration $configuration;
    private Client $httpClient;

    public function __construct(LoggerInterface $logger, StreamxClientConfiguration $configuration) {
        $this->logger = $logger;
        $this->configuration = $configuration;
        $this->httpClient = new Client();
    }

    /**
     * @param CloudEventInterface[] $cloudEvents
     * @return true if and only if all events are successfully ingested to, and responded with success by StreamX (false otherwise)
     */
    public function send(array $cloudEvents, int $storeId): bool {
        $keys = CloudEventUtils::extractSubjects($cloudEvents);
        $eventTypes = CloudEventUtils::extractEventTypes($cloudEvents);

        $baseUrl = $this->configuration->getIngestionBaseUrl($storeId);
        $streamxPublisher = $this->createStreamxPublisher($baseUrl, $storeId);
        $this->logger->info("Ingesting data with type " . json_encode($eventTypes) . " to store $storeId at $baseUrl with keys " . json_encode($keys));

        try {
            $streamxPublisher->sendMulti($cloudEvents, [
                RequestOptions::STREAM => true,
                RequestOptions::CONNECT_TIMEOUT => $this->configuration->getConnectionTimeout($storeId),
                RequestOptions::TIMEOUT => $this->configuration->getResponseTimeout($storeId),
                RequestOptions::VERIFY => !$this->configuration->shouldDisableCertificateValidation($storeId),
            ]);
            $this->logger->info('Finished ingesting data with success');
            return true;
        } catch (Exception $ex) {
            $this->logExceptionAsError('Finished ingesting data with failure', $ex);
            return false;
        }
    }

    private function createStreamxPublisher(string $baseUrl, int $storeId): Publisher {
        $ingestionClientBuilder = StreamxClientBuilders::create($baseUrl)
            ->setHttpClient($this->httpClient);

        $authToken = $this->configuration->getAuthToken($storeId);
        if ($authToken) {
            $ingestionClientBuilder->setAuthToken($authToken);
        }

        return $ingestionClientBuilder->build()->newPublisher();
    }
}
