# Security

This repository is a portfolio/demo project and is not a production payment
processor. It does not implement PCI DSS controls, card data handling, KYC/AML,
or a full authentication/authorization product.

## Reporting

If you find a security issue, please open a private security advisory on GitHub
instead of a public issue.

## Hardening already present

- API keys are stored as HMAC-SHA256 hashes with an environment pepper.
- SQL is parameterized; the service does not build SQL from user input.
- The API and worker containers run as a non-root distroless user. The local-only
  `ledgerctl`/k6 tool containers run as root so they can write mounted demo files.
- The admin port (metrics, pprof) is separate from the public API port.
- Request bodies are size-limited, unknown JSON fields are rejected.
- Journal entries are immutable at the database level.
- Secrets are read from the environment and are not logged.

## Before using in production

- Use TLS termination, a secret manager, and per-owner key rotation.
- Run PostgreSQL with least-privilege roles (migrator vs application).
- Replace the demo API-key model with OAuth2/mTLS or your platform's identity.
- Review the single-relay outbox design for your throughput and ordering needs.
- Add backups, PITR, and a tested disaster recovery plan.
