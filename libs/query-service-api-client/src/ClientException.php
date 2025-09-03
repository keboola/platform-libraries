<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

use Throwable;

class ClientException extends Exception
{
    /**
     * @var array<string, mixed>|null
     */
    private ?array $contextData;

    /**
     * @param array<string, mixed>|null $contextData
     */
    public function __construct(string $message, int $code = 0, ?Throwable $previous = null, ?array $contextData = null)
    {
        parent::__construct($message, $code, $previous);
        $this->contextData = $contextData;
    }

    /**
     * @return array<string, mixed>|null
     */
    public function getContextData(): ?array
    {
        return $this->contextData;
    }
}
