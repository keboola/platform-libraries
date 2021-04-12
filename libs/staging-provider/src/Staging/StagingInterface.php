<?php

namespace Keboola\StagingProvider\Staging;

interface StagingInterface
{
    /**
     * @return string
     */
    public static function getType();
}
