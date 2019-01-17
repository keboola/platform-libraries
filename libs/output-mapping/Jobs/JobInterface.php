<?php

namespace Keboola\OutputMapping\Jobs;

interface JobInterface
{
    public function run();
    public function isSynchronous();
}
