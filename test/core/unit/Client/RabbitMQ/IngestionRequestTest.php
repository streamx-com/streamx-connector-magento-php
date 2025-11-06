<?php

namespace StreamX\ConnectorCore\test\unit\Client\RabbitMQ;

use CloudEvents\V1\CloudEventInterface;
use PHPUnit\Framework\TestCase;
use StreamX\ConnectorCore\Client\Model\CloudEventUtils;
use StreamX\ConnectorCore\Client\Model\Data;
use StreamX\ConnectorCore\Client\RabbitMQ\IngestionRequest;

class IngestionRequestTest extends TestCase {

    private const STORE_ID = 5;

    /** @test */
    public function shouldConvertSingleEventToAndFromJson() {
        // given: prepare publish event
        $event = CloudEventUtils::createEvent(
            'publish-key',
            'publish',
            'source',
            new Data('Data to be published', 'type')
        );

        $ingestionRequest = new IngestionRequest([$event], self::STORE_ID);

        // when 1:
        $ingestionRequestAsJson = $ingestionRequest->toJson();

        // then
        $expectedIngestionRequestAsJson =
            <<< JSON
{
  "isBatch": false,
  "storeId": 5,
  "cloudEvents": {
    "specversion": "1.0",
    "id": "53fcb162-2969-4911-8862-dfb948ccdb9c",
    "source": "source",
    "type": "publish",
    "datacontenttype": "application/json",
    "subject": "publish-key",
    "time": "2025-10-30T16:52:04Z",
    "data": {
      "content": "RGF0YSB0byBiZSBwdWJsaXNoZWQ=",
      "type": "type"
    }
  }
}
JSON;
        $this->assertSameIngestionRequestJsons($expectedIngestionRequestAsJson, $ingestionRequestAsJson);

        // when 2:
        $recreatedIngestionRequest = IngestionRequest::fromJson($ingestionRequestAsJson);

        // then
        $this->assertSame($ingestionRequest->getStoreId(), $recreatedIngestionRequest->getStoreId());
        $this->assertSame($ingestionRequest->isBatch(), $recreatedIngestionRequest->isBatch());
        $this->assertSame(1, count($recreatedIngestionRequest->getCloudEvents()));
        $this->assertSameEvents($ingestionRequest->getCloudEvents()[0], $recreatedIngestionRequest->getCloudEvents()[0]);

        // when 3:
        $recreatedIngestionRequestAsJson = $recreatedIngestionRequest->toJson();

        // then
        $this->assertSameIngestionRequestJsons($ingestionRequestAsJson, $recreatedIngestionRequestAsJson);
    }

    /** @test */
    public function shouldConvertMultipleEventsToAndFromJson() {
        // given: prepare publish / unpublish events
        $event1 = CloudEventUtils::createEvent(
            'publish-key-1',
            'publish',
            'source-1',
            new Data('Data to be published 1', 'type-1')
        );
        $event2 = CloudEventUtils::createEvent(
            'publish-key-2',
            'publish',
            'source-2',
            new Data('Data to be published 2', 'type-2')
        );

        $event3 = CloudEventUtils::createEvent('unpublish-key-1',
            'unpublish',
            'source-3',
            new Data(null, 'type-3')
        );
        $event4 = CloudEventUtils::createEvent('unpublish-key-2',
            'unpublish',
            'source-4',
            new Data(null, 'type-4')
        );

        $ingestionRequest = new IngestionRequest([$event1, $event2, $event3, $event4], self::STORE_ID);

        // when 1:
        $ingestionRequestAsJson = $ingestionRequest->toJson();

        // then
        $expectedIngestionRequestAsJson =
<<< JSON
{
  "isBatch": true,
  "storeId": 5,
  "cloudEvents": [
    {
      "specversion": "1.0",
      "id": "53fcb162-2969-4911-8862-dfb948ccdb9c",
      "source": "source-1",
      "type": "publish",
      "datacontenttype": "application/json",
      "subject": "publish-key-1",
      "time": "2025-10-30T16:52:04Z",
      "data": {
        "content": "RGF0YSB0byBiZSBwdWJsaXNoZWQgMQ==",
        "type": "type-1"
      }
    },
    {
      "specversion": "1.0",
      "id": "89867983-7808-471a-800b-eaf3a65fd2c9",
      "source": "source-2",
      "type": "publish",
      "datacontenttype": "application/json",
      "subject": "publish-key-2",
      "time": "2025-10-30T16:52:04Z",
      "data": {
        "content": "RGF0YSB0byBiZSBwdWJsaXNoZWQgMg==",
        "type": "type-2"
      }
    },
    {
      "specversion": "1.0",
      "id": "73fa2277-5d3e-4c36-9449-9c797296aef7",
      "source": "source-3",
      "type": "unpublish",
      "datacontenttype": "application/json",
      "subject": "unpublish-key-1",
      "time": "2025-10-30T16:52:04Z",
      "data": {
        "content": null,
        "type": "type-3"
      }
    },
    {
      "specversion": "1.0",
      "id": "6ef51516-9353-44a8-88fb-092cc9d744f9",
      "source": "source-4",
      "type": "unpublish",
      "datacontenttype": "application/json",
      "subject": "unpublish-key-2",
      "time": "2025-10-30T16:52:04Z",
      "data": {
        "content": null,
        "type": "type-4"
      }
    }
  ]
}
JSON;
        $this->assertSameIngestionRequestJsons($expectedIngestionRequestAsJson, $ingestionRequestAsJson);

        // when 2:
        $recreatedIngestionRequest = IngestionRequest::fromJson($ingestionRequestAsJson);

        // then
        $this->assertSame($ingestionRequest->getStoreId(), $recreatedIngestionRequest->getStoreId());
        $this->assertSame($ingestionRequest->isBatch(), $recreatedIngestionRequest->isBatch());
        for ($i = 0; $i < count($recreatedIngestionRequest->getCloudEvents()); $i++) {
            $expectedEvent = $ingestionRequest->getCloudEvents()[$i];
            $actualEvent = $recreatedIngestionRequest->getCloudEvents()[$i];
            $this->assertSameEvents($expectedEvent, $actualEvent);
        }

        // when 3:
        $recreatedIngestionRequestAsJson = $recreatedIngestionRequest->toJson();

        // then
        $this->assertSameIngestionRequestJsons($ingestionRequestAsJson, $recreatedIngestionRequestAsJson);
    }

    private function assertSameIngestionRequestJsons(string $expected, string $actual) {
        $this->assertJsonStringEqualsJsonString(
            self::standardizeVariableValues($expected),
            self::standardizeVariableValues($actual)
        );
    }

    private static function standardizeVariableValues(string $json): string {
        $json = preg_replace('/"id": ?"[^"]+"/', '"id": "123"', $json);
        return preg_replace('/"time": ?"[^"]+"/', '"time": "2025-10-30T16:52:04Z"', $json);
    }

    private function assertSameEvents(CloudEventInterface $expected, CloudEventInterface $actual): void {
        $this->assertEquals($expected->getId(), $actual->getId());
        $this->assertEquals($expected->getSource(), $actual->getSource());
        $this->assertEquals($expected->getType(), $actual->getType());
        $this->assertEquals(json_encode($expected->getData()), json_encode($actual->getData()));
        $this->assertEquals($expected->getDataContentType(), $actual->getDataContentType());
        $this->assertEquals($expected->getDataSchema(), $actual->getDataSchema());
        $this->assertEquals($expected->getSubject(), $actual->getSubject());
        $this->assertEquals($expected->getTime()->format('Y-m-d H:i:s'), $actual->getTime()->format('Y-m-d H:i:s'));
        $this->assertEquals($expected->getExtensions(), $actual->getExtensions());
    }
}
