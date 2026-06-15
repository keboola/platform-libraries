<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

interface ResponseModelInterface
{
    /**
     * Build the model from a decoded API response body.
     *
     * `$data` is deliberately an untyped array (decoded JSON is untyped): implementers
     * cast/validate the values they need, without per-method type-narrowing annotations.
     */
    public static function fromResponseData(array $data): static;
}
