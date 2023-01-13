<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Staging\StagingInterface;

/**
 * @template T of StagingInterface
 * @implements StagingProviderInterface<T>
 */
abstract class AbstractStagingProvider implements StagingProviderInterface
{
    /** @var callable */
    private $stagingGetter;

    /** @var T */
    private $staging;

    /**
     * @param callable(): T $stagingGetter
     */
    public function __construct(callable $stagingGetter)
    {
        $this->stagingGetter = $stagingGetter;
    }

    /**
     * @return T
     */
    public function getStaging()
    {
        if ($this->staging !== null) {
            return $this->staging;
        }

        $stagingGetter = $this->stagingGetter;
        return $this->staging = $stagingGetter();
    }
}
