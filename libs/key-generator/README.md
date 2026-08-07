# Key Generator

Generates RSA key pairs for Keboola services — currently used for Snowflake key-pair authentication of
Connection workspaces (see [`keboola/staging-provider`](../staging-provider)).

Requires `ext-openssl`.

## Installation

```bash
composer require keboola/key-generator
```

## Usage

```php
use Keboola\KeyGenerator\PemKeyCertificateGenerator;

$generator = new PemKeyCertificateGenerator();

// Unencrypted private key
$pair = $generator->createPemKeyCertificate(null);

// Private key encrypted with a passphrase
$pair = $generator->createPemKeyCertificate('my-passphrase');

$pair->privateKey; // PEM-encoded 2048-bit RSA private key
$pair->publicKey;  // PEM-encoded public key
```

`createPemKeyCertificate()` returns a `PemKeyCertificatePair` with exactly two values — despite the name,
**no certificate is returned**. Internally it also builds a certificate signing request from
`CertificateSigningRequest::createDefault()` (hard-coded Keboola company details) and self-signs it, but
that certificate is currently discarded. Exposing it would require changing the return type and making the
CSR subject caller-supplied.

The generated key pair is not persisted anywhere; storing it is the caller's responsibility.
`PemKeyCertificatePair::$privateKey` and the `$password` argument are marked `#[SensitiveParameter]` so they
are redacted from stack traces.

Failures at any OpenSSL step throw a `RuntimeException`.

## Development

Run everything through the library's Docker Compose service:

```bash
docker compose run --rm dev-key-generator composer install
docker compose run --rm dev-key-generator composer ci   # validate + phpcs + phpstan + tests
```

No environment variables are required.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
