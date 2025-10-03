# PHP Storage Names Sanitizer

Sanitizes strings so that they are usable as column identifiers in [Keboola Connection Storage](https://help.keboola.com/storage/tables/).

## Usage

Method `sanitize` generates a string which is a safe column name:

```
use Keboola\StorageNamesSanitizer\ColumnNameSanitizer;

$sanitized = ColumnNameSanitizer::sanitize('my column name');
echo $sanitized; // prints 'my_column_name'
```

Method `toAscii` removes converts accented characters non-accented to fit into basic ASCII charset: 

```
use Keboola\StorageNamesSanitizer\ColumnNameSanitizer;

$sanitized = ColumnNameSanitizer::toAscii('test-vn-đá cuội');
echo $sanitized; // prints 'test_vn_da_cuoi'
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
