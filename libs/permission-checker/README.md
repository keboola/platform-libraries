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

## License

MIT licensed, see [LICENSE](./LICENSE) file.
