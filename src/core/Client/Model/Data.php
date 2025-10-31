<?php declare(strict_types=1);

namespace StreamX\ConnectorCore\Client\Model;

class Data {
    public ?string $content;
    public string $type;

    public function __construct(?string $content, string $type) {
        $this->content = $content ? base64_encode($content) : null;
        $this->type = $type;
    }
}