<?php

namespace Keboola\StagingProvider\Provider;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\StagingInterface;

/**
 * @template T of StagingInterface
 */
abstract class AbstractStagingProvider implements StagingProviderInterface
{
    /** @var callable */
    private $stagingGetter;

    /** @var T */
    private $staging;

    /** @var string */
    private $expectedStagingType;

    /**
     * @param callable(): T $stagingGetter
     * @param class-string<T> $expectedStagingType
     */
    public function __construct(callable $stagingGetter, $expectedStagingType)
    {
        if (!is_subclass_of($expectedStagingType, StagingInterface::class)) {
            throw new StagingProviderException(sprintf(
                'Staging type "%s" does not implement %s',
                $expectedStagingType,
                StagingInterface::class
            ));
        }

        $this->stagingGetter = $stagingGetter;
        $this->expectedStagingType = $expectedStagingType;
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
        $staging = $stagingGetter();

        if (!$staging instanceof $this->expectedStagingType) {
            throw new StagingProviderException(sprintf(
                'Staging getter must return instance of %s, %s returned.',
                $this->expectedStagingType,
                is_object($staging) ? get_class($staging) : gettype($staging)
            ));
        }

        return $this->staging = $staging;
    }
}
