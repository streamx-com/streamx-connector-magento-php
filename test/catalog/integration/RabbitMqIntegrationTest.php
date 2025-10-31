<?php

namespace StreamX\ConnectorCatalog\test\integration;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\AssertionFailedError;
use RuntimeException;
use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCatalog\test\integration\AppEntityUpdateStreamxPublishTests\BaseAppEntityUpdateTest;
use StreamX\ConnectorCore\Client\Model\CloudEventUtils;
use StreamX\ConnectorCore\Client\Model\Data;
use StreamX\ConnectorCore\Client\RabbitMQ\IngestionRequest;
use StreamX\ConnectorCore\Client\RabbitMQ\RabbitMqIngestionRequestsSender;
use StreamX\ConnectorCore\Client\RabbitMQ\RabbitMqQueuesManager;

/**
 * @inheritdoc
 */
class RabbitMqIntegrationTest extends BaseAppEntityUpdateTest {

    const INDEXER_IDS = [ProductIndexer::INDEXER_ID];

    private string $retryQueue1;
    private string $retryQueue2;
    private string $retryQueue3;
    private RabbitMqIngestionRequestsSender $rabbitMqSender;
    private const ingestionKey = 'key-1';

    protected function setUp(): void {
        parent::setUp();

        $this->retryQueue1 = RabbitMqQueuesManager::getRetryQueueName(1);
        $this->retryQueue2 = RabbitMqQueuesManager::getRetryQueueName(2);
        $this->retryQueue3 = RabbitMqQueuesManager::getRetryQueueName(3);

        $this->rabbitMqSender = $this->createRabbitMqSender();
        self::removeFromStreamX(self::ingestionKey);
    }

    private function createRabbitMqSender(): RabbitMqIngestionRequestsSender {
        $rabbitMqConfiguration = parent::createRabbitMqConfigurationMock();
        $logger = parent::createLoggerMock();
        return new RabbitMqIngestionRequestsSender($logger, $rabbitMqConfiguration);
    }

    /** @test */
    public function validIngestionEvent_ShouldBeProcessed() {
        // given
        $productJson = self::readValidationFileContent('original-bag-product.json');
        $validIngestionEvent = CloudEventUtils::createEvent(
            self::ingestionKey,
            self::PUBLISHING_EVENT_TYPE,
            self::EVENT_SOURCE,
            new Data($productJson, "product/bag")
        );
        $messagesCountBefore = self::getCurrentMessagesCount(RabbitMqQueuesManager::MAIN_QUEUE);
        $dlqMessagesCountBefore = self::getCurrentMessagesCount(RabbitMqQueuesManager::DEAD_LETTER_QUEUE);

        // when
        $this->rabbitMqSender->send(new IngestionRequest([$validIngestionEvent], parent::$store1Id));

        // then
        parent::assertExactDataIsPublished(self::ingestionKey, 'original-bag-product.json');

        // and: expecting none of the new messages to remain on queues
        self::assertCurrentMessagesCount(RabbitMqQueuesManager::MAIN_QUEUE, $messagesCountBefore);
        self::assertCurrentMessagesCount(RabbitMqQueuesManager::DEAD_LETTER_QUEUE, $dlqMessagesCountBefore);
    }

    /** @test */
    public function recoverableIngestionEvent_ShouldBeRetried() {
        // this is a long-running test, so execute it on demand only
        if (getenv('EXECUTE_LONG_RUNNING_TESTS') !== 'true') {
            $this->markTestSkipped('EXECUTE_LONG_RUNNING_TESTS env var is not true, skipping test');
        }

        // given
        $productJson = self::readValidationFileContent('original-bag-product.json');
        $validIngestionEvent = CloudEventUtils::createEvent(
            self::ingestionKey,
            self::PUBLISHING_EVENT_TYPE,
            self::EVENT_SOURCE,
            new Data($productJson, "product/bag")
        );

        $messagesCountBefore = self::getCurrentMessagesCount(RabbitMqQueuesManager::MAIN_QUEUE);
        $dlqMessagesCountBefore = self::getCurrentMessagesCount(RabbitMqQueuesManager::DEAD_LETTER_QUEUE);

        $retryQueue1MessagesCountBefore = self::getCurrentMessagesCount($this->retryQueue1);
        $retryQueue2MessagesCountBefore = self::getCurrentMessagesCount($this->retryQueue2);
        $retryQueue3MessagesCountBefore = self::getCurrentMessagesCount($this->retryQueue3);

        $retryQueue1TotalMessagesCountBefore = self::getTotalMessagesCount($this->retryQueue1);
        $retryQueue2TotalMessagesCountBefore = self::getTotalMessagesCount($this->retryQueue2);
        $retryQueue3TotalMessagesCountBefore = self::getTotalMessagesCount($this->retryQueue3);

        try {
            // when
            shell_exec('docker pause rest-ingestion');
            $this->rabbitMqSender->send(new IngestionRequest([$validIngestionEvent], parent::$store1Id));

            // then: the message should go through consecutive retry queues...
            $timeoutSeconds = 25;
            self::assertTotalMessagesCount($this->retryQueue1, $retryQueue1TotalMessagesCountBefore + 1, $timeoutSeconds);
            self::assertTotalMessagesCount($this->retryQueue2, $retryQueue2TotalMessagesCountBefore + 1, $timeoutSeconds);
            self::assertTotalMessagesCount($this->retryQueue3, $retryQueue3TotalMessagesCountBefore + 1, $timeoutSeconds);
        } finally {
            // and when: cause of the error is gone
            shell_exec('docker unpause rest-ingestion');
        }

        // then: the message should be processed
        parent::assertExactDataIsPublished(self::ingestionKey, 'original-bag-product.json');

        // and: expecting none of the new messages to remain on queues
        self::assertCurrentMessagesCount(RabbitMqQueuesManager::MAIN_QUEUE, $messagesCountBefore);
        self::assertCurrentMessagesCount(RabbitMqQueuesManager::DEAD_LETTER_QUEUE, $dlqMessagesCountBefore);

        self::assertCurrentMessagesCount($this->retryQueue1, $retryQueue1MessagesCountBefore);
        self::assertCurrentMessagesCount($this->retryQueue2, $retryQueue2MessagesCountBefore);
        self::assertCurrentMessagesCount($this->retryQueue3, $retryQueue3MessagesCountBefore);
    }

    /** @test */
    public function invalidIngestionEvent_ShouldBeImmediatelyRedirectedToDeadLetterQueue() {
        // given
        $rabbitMqMessage = new AMQPMessage('This is not an Ingestion Request JSON');
        $messagesCountBefore = self::getCurrentMessagesCount(RabbitMqQueuesManager::MAIN_QUEUE);
        $dlqMessagesCountBefore = self::getCurrentMessagesCount(RabbitMqQueuesManager::DEAD_LETTER_QUEUE);

        // when
        self::sendMessageManually($rabbitMqMessage);

        // then: expecting the message to land on the Dead Letter Queue
        self::assertCurrentMessagesCount(RabbitMqQueuesManager::MAIN_QUEUE, $messagesCountBefore);
        self::assertCurrentMessagesCount(RabbitMqQueuesManager::DEAD_LETTER_QUEUE, 1 + $dlqMessagesCountBefore);

        // cleanup - remove the DLQ message from queue
        $this->removeDeadLetteredMessageByContent(
            'This is not an Ingestion Request JSON',
        );
    }

    /**
     * Use this function to send an invalid message (that cannot be sent using rabbitMqSender)
     */
    private static function sendMessageManually(AMQPMessage $message): void {
        self::doWithChannel(function (AMQPChannel $channel) use ($message) {
            $channel->basic_publish($message, RabbitMqQueuesManager::MAIN_EXCHANGE, RabbitMqQueuesManager::MAIN_QUEUE_ROUTING_KEY);
        });
    }

    private static function getCurrentMessagesCount(string $queue): int {
        return self::doWithChannel(function (AMQPChannel $channel) use ($queue) {
            RabbitMqQueuesManager::ensureQueuesCreated($channel);
            $queueInfo = $channel->queue_declare(
                $queue,
                true, // passive = true: just check, don't create
                true,
                false,
                false
            );
            return $queueInfo[1];
        });
    }

    private static function assertCurrentMessagesCount(string $queue, int $expectedCount): void {
        self::waitUntil(function () use ($queue, $expectedCount) {
            $actualCount = self::getCurrentMessagesCount($queue);
            self::assertEquals($expectedCount, $actualCount);
        });
    }

    private static function getTotalMessagesCount(string $queue): int {
        $url = sprintf('http://%s:%s/api/queues/%s/%s',
            parent::RABBIT_MQ_HOST,
            parent::RABBIT_MQ_API_PORT,
            urlencode('/'),
            $queue
        );
        $authData = sprintf('%s:%s',
            parent::RABBIT_MQ_USER,
            parent::RABBIT_MQ_PASSWORD
        );

        $curlHandle = curl_init();
        curl_setopt($curlHandle, CURLOPT_URL, $url);
        curl_setopt($curlHandle, CURLOPT_RETURNTRANSFER, true); // return the response as a string
        curl_setopt($curlHandle, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
        curl_setopt($curlHandle, CURLOPT_USERPWD, $authData);

        $response = curl_exec($curlHandle);
        curl_close($curlHandle);

        $data = json_decode($response, true);
        return isset($data['message_stats']) ? intval($data['message_stats']['publish']) : 0;
    }

    private static function assertTotalMessagesCount(string $queue, int $expectedCount, int $timeoutSeconds): void {
        self::waitUntil(function () use ($queue, $expectedCount) {
            $actualCount = self::getTotalMessagesCount($queue);
            self::assertEquals($expectedCount, $actualCount);
        }, $timeoutSeconds, 500);
    }

    private function removeDeadLetteredMessageByContent(string $expectedMessageBody): void {
        self::doWithChannel(function (AMQPChannel $channel) use ($expectedMessageBody) {
            $queue = RabbitMqQueuesManager::DEAD_LETTER_QUEUE;
            $messagesCount = self::getCurrentMessagesCount($queue);
            for ($i = 0; $i < $messagesCount; $i++) {
                $message = $channel->basic_get($queue);
                $deliveryTag = $message->getDeliveryTag();
                if ($expectedMessageBody === $message->getBody()) {
                    $channel->basic_ack($deliveryTag); // removes message from queue
                    return;
                } else {
                    $channel->basic_nack($deliveryTag, false, true); // puts message back to queue (use requeue=true), it's not the one we want to remove
                }
            }
        });
    }

    private static function doWithChannel(callable $function) {
        $connection = new AMQPStreamConnection(
            BaseStreamxTest::RABBIT_MQ_HOST,
            BaseStreamxTest::RABBIT_MQ_PORT,
            BaseStreamxTest::RABBIT_MQ_USER,
            BaseStreamxTest::RABBIT_MQ_PASSWORD
        );
        $channel = $connection->channel();
        $result = $function($channel);
        $connection->close();
        return $result;
    }

    private static function waitUntil(callable $assertion, int $timeoutSeconds = 5, int $intervalMilliseconds = 100): void {
        $startTime = microtime(true);
        $lastException = null;

        while ((microtime(true) - $startTime) < $timeoutSeconds) {
            try {
                $assertion();
                return; // success, exit
            } catch (AssertionFailedError $e) {
                $lastException = $e;
                usleep($intervalMilliseconds * 1000); // wait before retrying
            }
        }

        // if we ran out of time, rethrow last exception
        if ($lastException !== null) {
            throw $lastException;
        }

        throw new RuntimeException("waitUntil timed out but no assertion was triggered");
    }
}