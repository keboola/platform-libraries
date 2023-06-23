<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Variables\Model;

class ListOptions
{
    /**
     * @param non-empty-string|null $key
     * @param array<string, non-empty-string>|null $attributes
     * @param int<0, max>|null $offset
     * @param int<1, max>|null $limit
     */
    public function __construct(
        public ?string $key = null,
        public ?array $attributes = null,
        public ?int $offset = null,
        public ?int $limit = null,
    ) {
    }

    public function asQueryString(): string
    {
        return http_build_query(array_filter([
            'key' => $this->key,
            'attributes' => $this->attributes,
            'offset' => $this->offset,
            'limit' => $this->limit,
        ]));
    }
}
