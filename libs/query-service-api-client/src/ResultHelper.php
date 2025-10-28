<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

use Keboola\QueryApi\Response\JobResultsResponse;
use Keboola\QueryApi\Response\Statement;

class ResultHelper
{
    public static function mapColumnNamesIntoData(JobResultsResponse $response): JobResultsResponse
    {
        $data = $response->getData();
        $columnNames = array_column($response->getColumns(), 'name');

        $transformedData = [];
        foreach ($data as $row) {
            $transformedRow = [];
            foreach ($row as $index => $value) {
                if (isset($columnNames[$index]) && is_string($columnNames[$index])) {
                    $transformedRow[$columnNames[$index]] = $value;
                }
            }
            $transformedData[] = $transformedRow;
        }

        return new JobResultsResponse(
            $response->getStatus(),
            $response->getNumberOfRows(),
            $response->getRowsAffected(),
            $transformedData,
            $response->getColumns(),
            $response->getMessage(),
        );
    }

    /**
     * @param Statement[] $statements
     */
    public static function extractAllStatementErrors(array $statements): string
    {
        $errors = [];
        foreach ($statements as $statement) {
            if ($statement->getError() !== null) {
                $err = trim($statement->getError());
                if ($err !== '') {
                    $errors[] = $err;
                }
            }
        }

        if (!$errors) {
            return 'Unknown error';
        }

        return implode("\n", $errors);
    }
}
