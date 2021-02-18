<?php

namespace Keboola\InputMapping\Helper;

class Tag
{
    /** @var string */
    private $name;

    /** @var string */
    private $matchType;

    /**
     * Tag constructor.
     * @param string $name
     * @param string $matchType
     */
    public function __construct($name, $matchType)
    {

        $this->name = $name;
        $this->matchType = $matchType;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function getMatchType()
    {
        return $this->matchType;
    }
}
