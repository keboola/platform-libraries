# PHP Storage Names Sanitizer

Sanitizes strings so that they are usable as column identifiers in [Keboola Connection Storage](https://help.keboola.com/storage/tables/).

## Usage

Method `sanitize` generates a string which is a safe column name:

```
use Keboola\StorageNamesSanitizer\ColumnNameSanitizer;

$sanitized = ColumnNameSanitizer::sanitize('my column name');
echo $sanitized; // prints 'my_column_name'
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
