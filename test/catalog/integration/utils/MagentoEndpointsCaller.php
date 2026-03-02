<?php

namespace StreamX\ConnectorCatalog\test\integration\utils;

use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;

class MagentoEndpointsCaller {

    private const MAGENTO_REST_API_BASE_URL = 'https://magento-app-1:444/rest/all/V1';

    private function __construct() {
        // no instances
    }

    public static function call(string $relativeUrl, array $params = []): string {
        $endpointUrl = self::MAGENTO_REST_API_BASE_URL . "/$relativeUrl?XDEBUG_SESSION_START=PHPSTORM";
        $jsonBody = json_encode($params);
        $headers = ['Content-Type' => 'application/json; charset=UTF-8'];

        $request = new Request('PUT', $endpointUrl, $headers, $jsonBody);
        $httpClient = new Client(['verify' => false]);
        $response = $httpClient->sendRequest($request);
        if ($response->getStatusCode() !== 200) {
            throw new Exception('Unexpected status code: ' . $response->getStatusCode());
        }

        $responseBody = (string) $response->getBody();
        if ($responseBody !== '[]') { // response is not void
            $responseBody = json_decode($responseBody);
        }

        return $responseBody;
    }
}