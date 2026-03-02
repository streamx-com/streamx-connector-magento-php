<?php

namespace StreamX\ConnectorTestEndpoints\Api;

interface InitControllerInterface {

    /**
     * Configures global settings to prepare test environment.
     * Sets up additional stores and websites required by integration tests.
     * The endpoint also enables StreamX Connector and RabbitMQ
     * @return string response status
     */
    public function init(): string;
}
