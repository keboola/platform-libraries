<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;

final readonly class RunFailureReason implements ResponseModelInterface
{
    public function __construct(
        public ?string $reason,
        public ?string $message,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        /**
         * @var array{
         *     reason: string|null,
         *     message: string|null,
         * } $data
         */
        return new self(
            $data['reason'],
            $data['message'],
        );
    }
}
