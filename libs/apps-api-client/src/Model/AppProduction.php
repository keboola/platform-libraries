<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

final readonly class AppProduction implements ResponseModelInterface
{
    /**
     * @param non-empty-string|null $desiredConfigVersion
     * @param non-empty-string|null $actualConfigVersion
     */
    public function __construct(
        public ?string $desiredDeploymentId,
        public ?string $actualDeploymentId,
        public ?string $desiredConfigVersion,
        public ?string $actualConfigVersion,
        public ?string $desiredCreatedAt,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'desiredDeploymentId');
        Assert::keyExists($data, 'actualDeploymentId');
        Assert::keyExists($data, 'desiredConfigVersion');
        Assert::keyExists($data, 'actualConfigVersion');
        Assert::keyExists($data, 'desiredCreatedAt');
        Assert::nullOrString($data['desiredDeploymentId']);
        Assert::nullOrString($data['actualDeploymentId']);
        Assert::nullOrStringNotEmpty($data['desiredConfigVersion']);
        Assert::nullOrStringNotEmpty($data['actualConfigVersion']);
        Assert::nullOrString($data['desiredCreatedAt']);

        return new self(
            $data['desiredDeploymentId'],
            $data['actualDeploymentId'],
            $data['desiredConfigVersion'],
            $data['actualConfigVersion'],
            $data['desiredCreatedAt'],
        );
    }
}
