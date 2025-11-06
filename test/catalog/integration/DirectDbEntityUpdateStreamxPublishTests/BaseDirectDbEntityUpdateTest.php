<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\test\integration\BaseStreamxConnectorPublishTest;

/**
 * @inheritdoc
 *
 * Base class for testing indexers in Update By Schedule mode.
 * In this mode, Magento's MView feature is used, that tracks down changes in database tables.
 * These tests insert/update/delete database rows directly and then manually trigger MView to verify if Product / Category data is published.
 * In real world scenario, MView is triggered by Magento's cron system, according to schedule
 */
abstract class BaseDirectDbEntityUpdateTest extends BaseStreamxConnectorPublishTest {
    const INDEXER_MODE = parent::UPDATE_BY_SCHEDULE;

    protected function setUp(): void {
        $this->markAllPendingMviewOperationsAsExecuted();
        parent::setUp();
    }

    private function markAllPendingMviewOperationsAsExecuted(): void {
        foreach (static::INDEXER_IDS as $indexerId) {
            $query = "
                UPDATE mview_state
                SET version_id = (
                    SELECT MAX(version_id)
                    FROM ${indexerId}_cl
                )
                WHERE view_id = '$indexerId'
            ";
            self::$db->execute($query);
        }
    }
}