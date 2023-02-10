<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use Keboola\AzureApiClient\Exception\ClientException;
use Psr\Log\LoggerInterface;
use Symfony\Component\Validator\Constraints\NotBlank;
use Symfony\Component\Validator\Constraints\Range;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validation;

class GuzzleClientFactory
{
    private const DEFAULT_USER_AGENT = 'Azure PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 10;
    private const ALLOWED_OPTIONS = ['backoffMaxTries', 'userAgent', 'middleware'];

    /**
     * @param callable|null $requestHandler
     */
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly mixed $requestHandler = null,
    ) {
    }

    /**
     * @param array{
     *     backoffMaxTries?: null|int<0, max>,
     *     userAgent?: null|string,
     *     middleware?: null|list<callable>,
     * }  $options
     */
    public function getClient(string $baseUrl, array $options = []): GuzzleClient
    {
        $options = $this->validateAndNormalizeOptions($baseUrl, $options);

        // Initialize handlers (start with those supplied in constructor)
        $handlerStack = HandlerStack::create($this->requestHandler ?? null);
        foreach ($options['middleware'] ?? [] as $middleware) {
            $handlerStack->push($middleware);
        }

        // Set exponential backoff
        $handlerStack->push(Middleware::retry(new RetryDecider(
            $options['backoffMaxTries'],
            $this->logger,
        )));

        // Set client logger
        $handlerStack->push(Middleware::log(
            $this->logger,
            new MessageFormatter(
                '{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}"' .
                ' {code} {res_header_Content-Length}'
            )
        ));

        // finally create the instance
        return new GuzzleClient([
            'base_uri' => $baseUrl,
            'handler' => $handlerStack,
            'headers' => ['User-Agent' => $options['userAgent'], 'Content-type' => 'application/json'],
            'connect_timeout' => 10,
            'timeout' => 120,
        ]);
    }

    public function validateAndNormalizeOptions(string $baseUrl, array $options): array
    {
        $validator = Validation::createValidator();
        $errors = $validator->validate($baseUrl, [new Url()]);
        $errors->addAll(
            $validator->validate($baseUrl, [new NotBlank()])
        );
        $unknownOptions = array_diff(array_keys($options), self::ALLOWED_OPTIONS);

        if ($unknownOptions) {
            throw new ClientException(sprintf(
                'Invalid options when creating client: %s. Valid options are: %s.',
                implode(', ', $unknownOptions),
                implode(', ', self::ALLOWED_OPTIONS)
            ));
        }

        if (!empty($options['backoffMaxTries'])) {
            $errors->addAll($validator->validate($options['backoffMaxTries'], [new Range(['min' => 0, 'max' => 100])]));
            $options['backoffMaxTries'] = (int) $options['backoffMaxTries'];
        } else {
            $options['backoffMaxTries'] = self::DEFAULT_BACKOFF_RETRIES;
        }

        if (empty($options['userAgent'])) {
            $options['userAgent'] = self::DEFAULT_USER_AGENT;
        }

        if ($errors->count() !== 0) {
            $messages = [];
            /** @var ConstraintViolationInterface $error */
            foreach ($errors as $error) {
                $value = $error->getInvalidValue();
                $messages[] = sprintf(
                    'Value "%s" is invalid: %s',
                    is_scalar($value) ? $value : get_debug_type($value),
                    $error->getMessage()
                );
            }
            throw new ClientException('Invalid options when creating client: ' . implode("\n", $messages));
        }

        return $options;
    }
}
