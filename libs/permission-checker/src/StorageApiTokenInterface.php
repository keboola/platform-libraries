<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

interface StorageApiTokenInterface
{
    /** @return non-empty-string[] */
    public function getFeatures(): array;

    /** @return non-empty-string|null */
    public function getRole(): ?string;

    /** @return non-empty-string[]|null */
    public function getAllowedComponents(): ?array;
}
