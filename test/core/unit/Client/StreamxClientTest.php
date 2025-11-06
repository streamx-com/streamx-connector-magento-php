<?php

namespace StreamX\ConnectorCore\test\unit\Client;

use CloudEvents\V1\CloudEventInterface;
use Magento\Store\Api\Data\StoreInterface;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use StreamX\ConnectorCatalog\Indexer\CategoryIndexer;
use StreamX\ConnectorCatalog\Indexer\ProductIndexer;
use StreamX\ConnectorCore\Client\RabbitMQ\RabbitMqConfiguration;
use StreamX\ConnectorCore\Client\RabbitMQ\RabbitMqIngestionRequestsSender;
use StreamX\ConnectorCore\Client\StreamxClient;
use StreamX\ConnectorCore\Client\StreamxClientConfiguration;
use StreamX\ConnectorCore\Client\StreamxIngestor;

class StreamxClientTest extends TestCase {

    private StoreInterface $storeMock;
    private StreamxClient $clientSpy;

    public function setUp(): void {
        $this->storeMock = $this->createMock(StoreInterface::class);
        $this->storeMock->method('getId')->willReturn(5);
        $this->storeMock->method('getCode')->willReturn('store_5');

        $streamxClientConfiguration = $this->createMock(StreamxClientConfiguration::class);
        $streamxClientConfiguration->method('getEventSource')->willReturn('test-source');
        $streamxClientConfiguration->method('getPublishingEventType')->willReturn('publish');
        $streamxClientConfiguration->method('getUnpublishingEventType')->willReturn('unpublish');

        $this->clientSpy = $this
            ->getMockBuilder(StreamxClient::class)
            ->setConstructorArgs([
                $this->createMock(LoggerInterface::class),
                $streamxClientConfiguration,
                $this->createMock(RabbitMqConfiguration::class),
                $this->createMock(RabbitMqIngestionRequestsSender::class),
                $this->createMock(StreamxIngestor::class)
            ])
            ->onlyMethods(['ingest']) // mock only this method to do nothing
            ->getMock();
    }

    /** @test */
    public function verifyPublishKeyAndDataTypeForSimpleProduct() {
        $product = ['id' => '1'];

        $this->publishAndVerifyCloudEvent(
            $product,
            ProductIndexer::INDEXER_ID,
            'store_5_product:1',
            'product/simple',
            '{"id":"1"}'
        );
    }

    /** @test */
    public function verifyPublishKeyAndDataTypeForConfigurableProduct() {
        $product = [
            'id' => '2',
            'variants' => [
                'id' => '10'
            ]
        ];

        $this->publishAndVerifyCloudEvent(
            $product,
            ProductIndexer::INDEXER_ID,
            'store_5_product:2',
            'product/master',
            '{"id":"2","variants":{"id":"10"}}'
        );
    }

    /** @test */
    public function verifyUnpublishKeyAndDataTypeForProduct() {
        $productId = 3;

        $this->unpublishAndVerifyCloudEvent(
            $productId,
            ProductIndexer::INDEXER_ID,
            'store_5_product:3',
            'product'
        );
    }

    /** @test */
    public function verifyPublishKeyAndDataTypeForCategory() {
        $category = ['id' => '4'];

        $this->publishAndVerifyCloudEvent(
            $category,
            CategoryIndexer::INDEXER_ID,
            'store_5_category:4',
            'category',
            '{"id":"4"}'
        );
    }

    /** @test */
    public function verifyUnpublishKeyAndDataTypeForCategory() {
        $categoryId = 4;

        $this->unpublishAndVerifyCloudEvent(
            $categoryId,
            CategoryIndexer::INDEXER_ID,
            'store_5_category:4',
            'category'
        );
    }

    private function publishAndVerifyCloudEvent(array $entityToIngest, string $sourceIndexerId, string $expectedKey, string $expectedDataType, string $expectedPayload) {
        $this->setupCloudEventVerification($sourceIndexerId, 'publish', $expectedKey, $expectedDataType, $expectedPayload);
        $this->clientSpy->publish([$entityToIngest], $sourceIndexerId, $this->storeMock);
    }

    private function unpublishAndVerifyCloudEvent(int $productId, string $sourceIndexerId, string $expectedKey, string $expectedDataType) {
        $this->setupCloudEventVerification($sourceIndexerId, 'unpublish', $expectedKey, $expectedDataType, null);
        $this->clientSpy->unpublish([$productId], $sourceIndexerId, $this->storeMock);
    }

    private function setupCloudEventVerification(string $sourceIndexerId, string $expectedEventType, string $expectedKey, string $expectedDataType, ?string $expectedPayload) {
        $this->clientSpy->expects($this->once())
            ->method('ingest')
            ->with(
                $this->callback(fn ($cloudEventArg) =>
                    $this->assertCloudEvent($cloudEventArg, $expectedKey, $expectedDataType, $expectedEventType, $expectedPayload)
                ),
                $expectedEventType,
                $sourceIndexerId
            );
    }

    private function assertCloudEvent(array $cloudEvents, string $expectedKey, string $expectedDataType, string $expectedEventType, ?string $expectedPayload): bool {
        $this->assertCount(1, $cloudEvents);
        $event = $cloudEvents[0];

        $this->assertInstanceOf(CloudEventInterface::class, $event);
        $this->assertEquals($expectedKey, $event->getSubject());
        $this->assertEquals($expectedDataType, $event->getData()->type);
        $this->assertEquals($expectedEventType, $event->getType());
        if ($expectedPayload) {
            $this->assertEquals($expectedPayload, base64_decode($event->getData()->content));
        } else {
            $this->assertNull($event->getData()->content);
        }
        return true;
    }
}
