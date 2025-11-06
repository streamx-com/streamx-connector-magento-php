<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client\RabbitMQ;

use Psr\Log\LoggerInterface;
use StreamX\ConnectorCore\Client\Model\CloudEventUtils;

/**
 * Sends Ingestion Requests to Rabbit MQ queue
 */
class RabbitMqIngestionRequestsSender {

    private LoggerInterface $logger;
    private RabbitMqConfiguration $rabbitMqConfiguration;

    public function __construct(LoggerInterface $logger, RabbitMqConfiguration $rabbitMqConfiguration) {
        $this->logger = $logger;
        $this->rabbitMqConfiguration = $rabbitMqConfiguration;
    }

    public function send(IngestionRequest $ingestionRequest) {
        $cloudEvents = $ingestionRequest->getCloudEvents();
        $storeId = $ingestionRequest->getStoreId();

        $eventsCount = count($cloudEvents);
        $ingestionKeys = json_encode(CloudEventUtils::extractSubjects($cloudEvents));
        $this->logger->info("Sending $eventsCount events with ingestion keys $ingestionKeys to RabbitMQ for store $storeId");

        $rabbitMqMessageBody = $ingestionRequest->toJson();
        $rabbitMqMessage = RabbitMqMessagesManager::createIngestionRequestMessage($rabbitMqMessageBody, $ingestionKeys);
        RabbitMqMessagesManager::sendIngestionRequestMessage($this->rabbitMqConfiguration, $rabbitMqMessage);
    }
}