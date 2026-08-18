# Permission Checker

Generic permissions checker that centralizes all permission checks in one place.

## Usage
Library provides `Keboola\PermissionChecker\StorageApiTokenInterface` interface that is expected to be implemented by
caller (or provided by some other compatible library). The token is then passed to `Keboola\PermissionChecker\PermissionChecker`
along with concrete checker for the action to be validated.

If the check passes, script execution continues normally. If the check fails, `Keboola\PermissionChecker\Exception\PermissionException`
is thrown with a message that describes the reason of the failure and is safe to be presented to a user.

```php
use Keboola\PermissionChecker\PermissionChecker;
use Keboola\PersmissionChecker\Checker\JobQueue\CanRunJob;

$storageToken = new MyStorageApiClass(...)

$checker = new PermissionChecker();
$checker->checkPermissions($storageToken, new CanRunJob(BranchType::DEFAULT, 'keboola.component-id'));
```

## Development
Prerequisites:
* installed `docker` to run & develop the library

TL;DR:
```
docker compose run --rm dev-permission-checker composer install
docker compose run --rm dev-permission-checker composer ci
```

## Implementing new checker
Each action that needs to be validated has its own checker - a class implementing
`Keboola\PermissionChecker\Checker\PermissionCheckerInterface`. The interface has just a single method `checkPermission`
which receives `Keboola\PersmissionChecker\StorageApiToken` instance 
(different class that the token passed to `Keboola\PermissionChecker\PermissionChecker::checkPermissions()`!) 
and throws `Keboola\PermissionChecker\Exception\PermissionException` if the check fails.

If the checker requires any additional data or depends on some other service, it is free to require it through its constructor.

## Role-less (machine) tokens

A project storage token minted through the Manage API (`POST /manage/projects/{id}/tokens`) has no user
behind it, so `tokens/verify` returns no `admin` block and `StorageApiToken::getRole()` resolves to
`Role::NONE` — `getRole()` falls back to `Role::NONE` whenever the verify payload carries no role. Any
other token whose payload carries no role resolves the same way, and the check cannot tell them apart.

`Check\Notification\CanModifySubscriptions` treats such a token like any other non-`readOnly` token:

| Project | `Role::NONE` may modify subscriptions |
| --- | --- |
| regular project | yes, on any branch type |
| `protected-default-branch` project | no, on any branch type |

The non-SOX branch of the check is deliberately identical to `Check\OAuth\CanCreateAuthorization` — only
`Role::READ_ONLY` is denied, everything else is allowed — so no new authorisation pattern is introduced. In a
`protected-default-branch` project `Role::NONE` has no SOX role to map onto a branch, so it falls through to
`default => false` and stays denied exactly as before.

The trade-off, stated plainly: in a regular project **every** role-less token can now create and delete
notification subscriptions, not just automation that was meant to. This is accepted because
`Check\JobQueue\CanRunJob` already lets `Role::NONE` run jobs in a regular project without any role or
token-permission gate — it denies only `Role::READ_ONLY` — and running a job is the more powerful capability of
the two. If a narrower rule is ever needed, gate it on a token permission (`TokenPermission`) the way
`CanRunJob` does inside its `protected-default-branch` path.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
