# Output Mapping 
[![Build Status](https://travis-ci.com/keboola/output-mapping.svg?branch=master)](https://travis-ci.com/keboola/output-mapping) 
[![Code Climate](https://codeclimate.com/github/keboola/output-mapping/badges/gpa.svg)](https://codeclimate.com/github/keboola/output-mapping) 
[![Test Coverage](https://codeclimate.com/github/keboola/output-mapping/badges/coverage.svg)](https://codeclimate.com/github/keboola/output-mapping/coverage)

Output mapping library for Docker Runner. Library processes output mapping, imports data to Storage tables into CSV files and files to Storage Files. 
Files are imported from local directory.


## Development

Create `.env.local` file from this `.env` template and fill the missing envs:

```ini
cp .env .env.local
```

To run Synapse tests, set RUN_SYNAPSE_TESTS to 1 and supply a Storage API token to a project with Synapse backend. Synapse tests are by default skipped (unless the above env is set).

## License

MIT licensed, see [LICENSE](./LICENSE) file.
