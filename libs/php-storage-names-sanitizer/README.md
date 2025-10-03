# Sanitizer [![Build Status](https://travis-ci.com/keboola/sanitizer.svg?branch=master)](https://travis-ci.com/keboola/sanitizer) [![Maintainability](https://api.codeclimate.com/v1/badges/52976b1304fa6203cdab/maintainability)](https://codeclimate.com/github/keboola/sanitizer/maintainability)  [![Test Coverage](https://api.codeclimate.com/v1/badges/52976b1304fa6203cdab/test_coverage)](https://codeclimate.com/github/keboola/sanitizer/test_coverage)

Sanitizes strings so that they are usable as column identifiers in [Keboola Connection Storage](https://help.keboola.com/storage/tables/).

## Usage

Method `sanitize` generates a string which is a safe column name:

```
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

$sanitized = ColumnNameSanitizer::sanitize('my column name');
echo $sanitized; // prints 'my_column_name'
```

Method `toAscii` removes converts accented characters non-accented to fit into basic ASCII charset: 

```
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

$sanitized = ColumnNameSanitizer::toAscii('test-vn-đá cuội');
echo $sanitized; // prints 'test_vn_da_cuoi'
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
