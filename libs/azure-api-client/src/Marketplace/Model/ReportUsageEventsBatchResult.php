<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace\Model;

use Keboola\AzureApiClient\ResponseModelInterface;

final class ReportUsageEventsBatchResult implements ResponseModelInterface
{
    public function __construct(
        /** @var UsageEventResult[] */
        public readonly array $result,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self(
            array_map(UsageEventResult::fromResponseData(...), $data['result']),
        );
    }
}
