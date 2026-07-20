<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

final class SubmitQueryJobResponse implements ResponseModelInterface
{
    public function __construct(readonly string $queryJobId)
    {
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'queryJobId');
        Assert::string($data['queryJobId']);

        return new self($data['queryJobId']);
    }

    public function getQueryJobId(): string
    {
        return $this->queryJobId;
    }
}
