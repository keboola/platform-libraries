<?php

namespace Keboola\OutputMapping\Staging;

use Keboola\OutputMapping\Lister\ListerInterface;
use Keboola\OutputMapping\Lister\LocalLister;
use Keboola\OutputMapping\SourcesValidator\LocalSourcesValidator;
use Keboola\OutputMapping\SourcesValidator\SourcesValidatorInterface;

class StagingFactory
{
    public function __construct(private readonly string $path)
    {
    }

    public function getLister(): ListerInterface
    {
        //TODO throw new \Exception('Not implemented');
        return new LocalLister($this->path);
    }

    public function getSourcesValidator(): SourcesValidatorInterface
    {
        return new LocalSourcesValidator();
        // TODO throw new \Exception('Not implemented');
    }
}
