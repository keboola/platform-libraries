<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\ClientCredentialsAuth;
use Keboola\AzureApiClient\Authentication\Authenticator\SharedAccessSignatureAuth;
use Keboola\AzureApiClient\ServiceBus\Model\ReceivedMessage;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use RuntimeException;

/**
 * Access to the Azure resources provisioned by provisioning/ for tests in the "e2e" group.
 */
final class E2ETestEnvironment
{
    private const ENV_PREFIX = 'AZURE_API_CLIENT_TEST__';
    private const RECEIVE_WAIT_SECONDS = 5;

    /**
     * @return non-empty-string
     */
    public static function get(string $name): string
    {
        $value = getenv(self::ENV_PREFIX . $name);
        if ($value === false || $value === '') {
            throw new RuntimeException(sprintf(
                'Environment variable "%s" is not set, see "Development" in README.md',
                self::ENV_PREFIX . $name,
            ));
        }
        return $value;
    }

    public static function entraAuth(): ClientCredentialsAuth
    {
        return new ClientCredentialsAuth(
            self::get('TENANT_ID'),
            self::get('CLIENT_ID'),
            self::get('CLIENT_SECRET'),
        );
    }

    public static function serviceBusWithSas(): ServiceBusApiClient
    {
        return new ServiceBusApiClient(
            self::get('SERVICE_BUS_ENDPOINT'),
            new ApiClientConfiguration(
                authenticator: new SharedAccessSignatureAuth(
                    self::get('SERVICE_BUS_SAS_KEY_NAME'),
                    self::get('SERVICE_BUS_SAS_KEY'),
                ),
            ),
        );
    }

    /**
     * Receives from the queue until a message matching $predicate arrives. Messages that do not match are
     * leftovers of earlier runs and are deleted.
     *
     * @param callable(ReceivedMessage): bool $predicate
     */
    public static function receiveMatching(
        ServiceBusApiClient $client,
        string $queueName,
        callable $predicate,
        int $timeoutSeconds,
    ): ReceivedMessage {
        $deadline = time() + $timeoutSeconds;
        do {
            $message = $client->peekLockMessage($queueName, self::RECEIVE_WAIT_SECONDS);
            if ($message !== null) {
                if ($predicate($message)) {
                    return $message;
                }
                $client->deleteMessage($message->lockLocation);
            }
        } while (time() < $deadline);

        throw new RuntimeException(sprintf(
            'No matching message arrived to queue "%s" within %d seconds',
            $queueName,
            $timeoutSeconds,
        ));
    }
}
