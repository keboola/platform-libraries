<?php

declare(strict_types=1);

namespace Keboola\LoggingBundle;

use Monolog\Processor\ProcessorInterface;

use function DDTrace\current_context;

class DataDogContextProcessor implements ProcessorInterface
{
    public function __invoke(array $record): array
    {
        if (!function_exists('DDTrace\current_context')) {
            return $record;
        }

        $context = current_context();

        $record['dd'] = [
            'trace_id' => $context['trace_id'],
            'span_id'  => $context['span_id'],
        ];

        return $record;
    }
}
