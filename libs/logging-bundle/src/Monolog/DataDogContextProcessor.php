<?php

declare(strict_types=1);

namespace Keboola\LoggingBundle\Monolog;

use Monolog\LogRecord;
use Monolog\Processor\ProcessorInterface;

use function DDTrace\current_context;

class DataDogContextProcessor implements ProcessorInterface
{
    public function __invoke(LogRecord $record): LogRecord
    {
        $context = current_context();

        $record->extra['dd'] = [
            'trace_id' => $context['trace_id'],
            'span_id'  => $context['span_id'],
        ];

        return $record;
    }
}
