<?php

namespace Keboola\OutputMapping\SourcesValidator;

interface SourcesValidatorInterface
{
    public function validate(array $manifests, array $dataItems): void;
}
