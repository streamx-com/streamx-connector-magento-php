<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client;

use GuzzleHttp\Client;
use GuzzleHttp\RequestOptions;
use Psr\Log\LoggerInterface;
use Streamx\Clients\Ingestion\Builders\StreamxClientBuilders;
use Streamx\Clients\Ingestion\Exceptions\StreamxClientException;
use Streamx\Clients\Ingestion\Publisher\MessageStatus;
use Streamx\Clients\Ingestion\Publisher\Message;
use Streamx\Clients\Ingestion\Publisher\Publisher;
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
     * @param Message[] $ingestionMessages
     * @return true if and only if all messages are successfully ingested to, and responded with success by StreamX (false otherwise)
     * @throws StreamxClientException
     */
    public function send(array $ingestionMessages, int $storeId): bool {
        $keys = array_column($ingestionMessages, 'key');
        $action = implode(', ', array_unique(array_column($ingestionMessages, 'action')));

        $baseUrl = $this->configuration->getIngestionBaseUrl($storeId);
        $streamxPublisher = $this->createStreamxPublisher($baseUrl, $storeId);
        $this->logger->info("Ingesting data with action $action to store $storeId at $baseUrl with keys " . json_encode($keys));

        $messageStatuses = $streamxPublisher->sendMulti($ingestionMessages, [
            RequestOptions::STREAM => true,
            RequestOptions::CONNECT_TIMEOUT => $this->configuration->getConnectionTimeout($storeId),
            RequestOptions::TIMEOUT => $this->configuration->getResponseTimeout($storeId),
            RequestOptions::VERIFY => !$this->configuration->shouldDisableCertificateValidation($storeId),
        ]);

        $success = $this->isEachStatusSuccess($ingestionMessages, $messageStatuses);
        $this->logger->info('Finished ingesting data with ' . ($success ? 'success' : 'failure'));
        return $success;
    }

    private function createStreamxPublisher(string $baseUrl, int $storeId): Publisher {
        $ingestionClientBuilder = StreamxClientBuilders::create($baseUrl)
            ->setHttpClient($this->httpClient);

        $authToken = $this->configuration->getAuthToken($storeId);
        if ($authToken) {
            $ingestionClientBuilder->setAuthToken($authToken);
        }

        return $ingestionClientBuilder->build()->newPublisher(
            $this->configuration->getChannelName($storeId),
            $this->configuration->getChannelSchemaName($storeId)
        );
    }

    /**
     * @param Message[] $inputMessages
     * @param MessageStatus[] $responses
     */
    private function isEachStatusSuccess(array $inputMessages, array $responses): bool {
        $inputMessagesCount = count($inputMessages);
        $responsesCount = count($responses);

        $sameNumberOfResponsesAndMessages = $responsesCount === $inputMessagesCount;
        if (!$sameNumberOfResponsesAndMessages) {
            $this->logger->warning("Received $responsesCount responses for $inputMessagesCount messages");
        }

        $success = true;
        for ($i = 0; $i < $responsesCount; $i++) {
            $response = $responses[$i];
            if ($response->getSuccess() === null) {
                $success = false;

                $errorMessage = 'Ingestion failure: ' . json_encode($response->getFailure());
                if ($sameNumberOfResponsesAndMessages) {
                    $inputMessage = $inputMessages[$i];
                    $errorMessage .= ". The failing message:\n" . json_encode($inputMessage);
                }
                $this->logger->error($errorMessage);
            }
        }
        return $success;
    }
}
