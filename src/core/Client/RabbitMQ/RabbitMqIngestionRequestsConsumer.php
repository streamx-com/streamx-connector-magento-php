<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client\RabbitMQ;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Streamx\Clients\Ingestion\Exceptions\StreamxClientException;
use StreamX\ConnectorCore\Client\StreamxIngestor;
use StreamX\ConnectorCore\Console\Command\OutputInterfaceLoggerWrapper;
use StreamX\ConnectorCore\Traits\ExceptionLogger;
use Throwable;

/**
 * Consumes Ingestion Requests from Rabbit MQ queue and executes them
 */
class RabbitMqIngestionRequestsConsumer {
    use ExceptionLogger;

    private const CHECK_IF_RABBIT_MQ_IS_ENABLED_INTERVAL_SECONDS = 2;
    private const CONSUME_MESSAGES_BATCH_INTERVAL_MICROSECONDS = 250_000;
    private const MAX_MESSAGES_IN_ONE_ITERATION = 100;

    private LoggerInterface $logger;
    private StreamxIngestor $streamxIngestor;
    private RabbitMqConfiguration $rabbitMqConfiguration;

    public function __construct(
        LoggerInterface $logger,
        StreamxIngestor $streamxIngestor,
        RabbitMqConfiguration $rabbitMqConfiguration
    ) {
        $this->logger = $logger;
        $this->streamxIngestor = $streamxIngestor;
        $this->rabbitMqConfiguration = $rabbitMqConfiguration;
    }

    public function start(OutputInterfaceLoggerWrapper $loggerWrapper): void {
        while (true) {
            if ($this->rabbitMqConfiguration->isEnabled()) {
                try {
                    $this->consumeMessages($loggerWrapper);
                } catch (Throwable $e) {
                    $this->logExceptionAsError('Error while consuming messages', $e);
                }
                usleep(self::CONSUME_MESSAGES_BATCH_INTERVAL_MICROSECONDS);
            } else {
                sleep(self::CHECK_IF_RABBIT_MQ_IS_ENABLED_INTERVAL_SECONDS);
            }
        }
    }

    private function consumeMessages(OutputInterfaceLoggerWrapper $loggerWrapper): void {
        RabbitMqMessagesManager::doWithChannel($this->rabbitMqConfiguration, function (AMQPChannel $channel) use ($loggerWrapper) {
            for ($i = 0; $i < self::MAX_MESSAGES_IN_ONE_ITERATION; $i++) {
                $message = $channel->basic_get(RabbitMqQueuesManager::MAIN_QUEUE);
                if ($message) {
                    $this->consumeMessage($message, $channel, $loggerWrapper);
                    $channel->basic_ack($message->getDeliveryTag()); // remove from source queue
                } else { // no more messages on the queue currently
                    break;
                }
            }
        });
    }

    private function consumeMessage(AMQPMessage $message, AMQPChannel $channel, OutputInterfaceLoggerWrapper $loggerWrapper): void {
        $ingestionKeys = RabbitMqMessagesManager::readIngestionKeys($message);
        $this->logger->info("Consuming message with ingestion keys $ingestionKeys");

        $messageBody = $message->getBody();
        $messageBodyBeginning = substr($messageBody, 0, 100) . ' (...)';

        try {
            $ingestionRequest = IngestionRequest::fromJson($messageBody);
        } catch (Throwable $e) {
            $this->logExceptionAsError("Error deserializing IngestionRequest from $messageBodyBeginning", $e);
            $loggerWrapper->errorToCommandConsole('Invalid IngestionRequest JSON');
            $this->transferToDeadLetterQueue($message, $channel); // no use to retry invalid json message
            return;
        }

        try {
            $success = $this->sendToStreamX($ingestionRequest);
        } catch (Throwable $e) {
            $this->logExceptionAsError("Error sending the message to StreamX: $messageBodyBeginning", $e);
            $loggerWrapper->errorToCommandConsole('Error sending to StreamX: ' . $e->getMessage());
            $this->transferToRetryQueue($message, $channel); // retry, reason may be a recoverable/temporary connection loss
            return;
        }

        if (!$success) {
            $this->logger->error("Error response from StreamX to the message: $messageBodyBeginning");
            $loggerWrapper->errorToCommandConsole('Error response from StreamX');
            $this->transferToRetryQueue($message, $channel); // retry, reason may be some recoverable internal StreamX issue
        }
    }

    /**
     * @throws StreamxClientException
     */
    private function sendToStreamX(IngestionRequest $ingestionRequest): bool {
        return $this->streamxIngestor->send(
            $ingestionRequest->getCloudEvents(),
            $ingestionRequest->getStoreId()
        );
    }

    private function transferToDeadLetterQueue(AMQPMessage $message, AMQPChannel $channel) {
        $channel->basic_publish(
            $message,
            RabbitMqQueuesManager::DEAD_LETTER_EXCHANGE,
            RabbitMqQueuesManager::DEAD_LETTER_ROUTING_KEY
        );
    }

    private function transferToRetryQueue(AMQPMessage $message, AMQPChannel $channel) {
        $retryCount = RabbitMqMessagesManager::readRetryCount($message);
        if ($retryCount >= RabbitMqQueuesManager::MAX_RETRIES_COUNT) {
            $this->logger->error('Max retries count has been reached, transferring the message to Dead Letter Queue');
            $this->transferToDeadLetterQueue($message, $channel);
        } else {
            $retryCount = RabbitMqMessagesManager::increaseRetryCount($message);
            $routingKey = RabbitMqQueuesManager::getRetryQueueRoutingKey($retryCount);
            $this->logger->warning("Retrying the message by republishing with routing key $routingKey");
            $channel->basic_publish(
                $message,
                RabbitMqQueuesManager::RETRY_EXCHANGE,
                $routingKey
            );
        }
    }
}
