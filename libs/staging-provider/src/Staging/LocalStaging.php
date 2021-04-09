<?php

namespace Keboola\StagingProvider\Staging;

class LocalStaging implements StagingInterface
{
    /** @var string */
    private $path;

    /**
     * @param string $path
     */
    public function __construct($path)
    {
        $this->path = $path;
    }

    public static function getType()
    {
        return 'local';
    }

    public function getPath()
    {
        return $this->path;
    }
}
