<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

class PrivateKeyWorkspaceCredentials implements CredentialsInterface
{
    private function __construct(
        private readonly string $privateKey,
    ) {
    }

    /**
     * @param array{
     *     privateKey: string,
     * } $credentials
     */
    public static function fromPasswordResetArray(array $credentials): self
    {
        return new self(
            $credentials['privateKey'],
        );
    }

    public function toArray(): array
    {
        return [
            'privateKey' => $this->privateKey,
        ];
    }
}
