# Slicer
Library which installs the Slicer tool from 
[https://github.com/keboola/processor-split-table/releases/](https://github.com/keboola/processor-split-table/releases/).

## Usage
Add `Keboola\\Slicer\\Slicer::installSlicer` command to composer.json, e.g.:

```json
    "pre-autoload-dump": [
      "Aws\\Script\\Composer\\Composer::removeUnusedServices",
      "Keboola\\Slicer\\Slicer::installSlicer"
    ]
```

Then run `composer install keboola/slicer`. This will download the slicer tool to `bin/slicer`.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
