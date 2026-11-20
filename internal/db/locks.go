package db

// Advisory lock keys must remain stable across PostgreSQL versions, deployments
// and rolling restarts. hashtext() is an internal function whose result is not
// guaranteed to stay stable, so all application-level locks use fixed bigint
// keys instead.
const (
	OutboxRelayLockKey    int64 = 0x4c45444745524f42 // "LEDGEROB"
	ReconciliationLockKey int64 = 0x4c45444745524f52 // "LEDGEROR"
)
