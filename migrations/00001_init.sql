-- +goose Up
CREATE TABLE accounts (
    id              uuid PRIMARY KEY,
    owner_id        text NOT NULL,
    name            text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    currency        char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    type            text NOT NULL CHECK (type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
    status          text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'frozen', 'closed')),
    allow_negative  boolean NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX accounts_owner_idx ON accounts (owner_id, created_at DESC, id DESC);
CREATE UNIQUE INDEX accounts_owner_name_uq ON accounts (owner_id, name) WHERE status <> 'closed';

CREATE TABLE account_balances (
    account_id      uuid PRIMARY KEY REFERENCES accounts (id),
    balance_minor   bigint NOT NULL DEFAULT 0,
    held_minor      bigint NOT NULL DEFAULT 0 CHECK (held_minor >= 0),
    version         bigint NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE journal_transactions (
    id              uuid PRIMARY KEY,
    type            text NOT NULL CHECK (type IN ('transfer', 'hold_capture', 'adjustment', 'fee', 'reversal')),
    status          text NOT NULL DEFAULT 'posted' CHECK (status IN ('posted', 'reversed')),
    description     text NOT NULL DEFAULT '',
    metadata        jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX journal_transactions_created_idx ON journal_transactions (created_at DESC, id DESC);

CREATE TABLE journal_entries (
    id              uuid PRIMARY KEY,
    transaction_id  uuid NOT NULL REFERENCES journal_transactions (id),
    account_id      uuid NOT NULL REFERENCES accounts (id),
    direction       text NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_minor    bigint NOT NULL CHECK (amount_minor > 0),
    currency        char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX journal_entries_account_idx
    ON journal_entries (account_id, created_at DESC, id DESC);
CREATE INDEX journal_entries_transaction_idx ON journal_entries (transaction_id);

-- The database, not the application, is the last line of defence for the
-- central accounting invariant: every journal transaction balances per currency.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION assert_journal_balanced() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    bad_currency char(3);
    account_currency char(3);
BEGIN
    SELECT a.currency INTO account_currency FROM accounts a WHERE a.id = NEW.account_id;
    IF account_currency IS NULL THEN
        RAISE EXCEPTION 'account % does not exist', NEW.account_id USING ERRCODE = '23503';
    END IF;
    IF account_currency <> NEW.currency THEN
        RAISE EXCEPTION 'entry currency % does not match account currency %', NEW.currency, account_currency
            USING ERRCODE = '23514';
    END IF;

    SELECT e.currency INTO bad_currency
    FROM journal_entries e
    WHERE e.transaction_id = NEW.transaction_id
    GROUP BY e.currency
    HAVING sum(CASE e.direction WHEN 'debit' THEN e.amount_minor ELSE -e.amount_minor END) <> 0
    LIMIT 1;

    IF bad_currency IS NOT NULL THEN
        RAISE EXCEPTION 'journal transaction % is unbalanced for currency %',
            NEW.transaction_id, bad_currency USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER journal_entries_balanced
    AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_journal_balanced();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION forbid_journal_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'journal entries are immutable' USING ERRCODE = '42501';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER journal_entries_immutable
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_journal_mutation();

CREATE TABLE transfers (
    id                      uuid PRIMARY KEY,
    owner_id                text NOT NULL,
    idempotency_key         text NOT NULL,
    transaction_id          uuid NOT NULL UNIQUE REFERENCES journal_transactions (id),
    source_account_id       uuid NOT NULL REFERENCES accounts (id),
    destination_account_id  uuid NOT NULL REFERENCES accounts (id),
    amount_minor            bigint NOT NULL CHECK (amount_minor > 0),
    currency                char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    description             text NOT NULL DEFAULT '',
    status                  text NOT NULL DEFAULT 'posted' CHECK (status IN ('posted', 'reversed')),
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (owner_id, idempotency_key),
    CHECK (source_account_id <> destination_account_id)
);

CREATE INDEX transfers_source_idx ON transfers (source_account_id, created_at DESC, id DESC);
CREATE INDEX transfers_destination_idx ON transfers (destination_account_id, created_at DESC, id DESC);

CREATE TABLE holds (
    id                      uuid PRIMARY KEY,
    owner_id                text NOT NULL,
    idempotency_key         text NOT NULL,
    account_id              uuid NOT NULL REFERENCES accounts (id),
    amount_minor            bigint NOT NULL CHECK (amount_minor > 0),
    currency                char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status                  text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'captured', 'released', 'expired')),
    expires_at              timestamptz NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    captured_transaction_id uuid REFERENCES journal_transactions (id),
    UNIQUE (owner_id, idempotency_key)
);

CREATE INDEX holds_active_expiry_idx ON holds (expires_at, id) WHERE status = 'active';
CREATE INDEX holds_account_idx ON holds (account_id, status);

CREATE TABLE idempotency_keys (
    owner_id        text NOT NULL,
    key             text NOT NULL,
    request_hash    bytea NOT NULL,
    method          text NOT NULL,
    path            text NOT NULL,
    status_code     integer,
    response_body   jsonb,
    resource_type   text,
    resource_id     uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    PRIMARY KEY (owner_id, key)
);

CREATE INDEX idempotency_expiry_idx ON idempotency_keys (expires_at);

CREATE TABLE outbox (
    id              bigserial PRIMARY KEY,
    event_id        uuid NOT NULL UNIQUE,
    aggregate_type  text NOT NULL,
    aggregate_id    uuid NOT NULL,
    event_type      text NOT NULL,
    payload         jsonb NOT NULL,
    headers         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    available_at    timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz,
    attempts        integer NOT NULL DEFAULT 0,
    last_error      text
);

CREATE INDEX outbox_pending_idx ON outbox (available_at, id) WHERE published_at IS NULL;
CREATE INDEX outbox_aggregate_idx ON outbox (aggregate_id, id);

CREATE TABLE processed_events (
    consumer        text NOT NULL,
    event_id        uuid NOT NULL,
    processed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE audit_events (
    id              bigserial PRIMARY KEY,
    event_id        uuid NOT NULL UNIQUE,
    event_type      text NOT NULL,
    aggregate_id    uuid NOT NULL,
    payload         jsonb NOT NULL,
    consumed_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reconciliation_runs (
    id                  uuid PRIMARY KEY,
    started_at          timestamptz NOT NULL,
    finished_at         timestamptz,
    status              text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    checked_accounts    bigint NOT NULL DEFAULT 0,
    discrepancy_count   bigint NOT NULL DEFAULT 0,
    error               text,
    stats               jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE reconciliation_discrepancies (
    id              bigserial PRIMARY KEY,
    run_id          uuid NOT NULL REFERENCES reconciliation_runs (id) ON DELETE CASCADE,
    kind            text NOT NULL,
    account_id      uuid,
    transaction_id  uuid,
    expected        bigint,
    actual          bigint,
    details         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX reconciliation_discrepancies_run_idx ON reconciliation_discrepancies (run_id, kind);

CREATE TABLE api_keys (
    id              uuid PRIMARY KEY,
    owner_id        text NOT NULL,
    name            text NOT NULL,
    key_prefix      text NOT NULL,
    key_hash        bytea NOT NULL UNIQUE,
    scopes          text[] NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL DEFAULT now(),
    revoked_at      timestamptz
);

CREATE INDEX api_keys_owner_idx ON api_keys (owner_id);

-- +goose Down
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS reconciliation_discrepancies;
DROP TABLE IF EXISTS reconciliation_runs;
DROP TABLE IF EXISTS audit_events;
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS holds;
DROP TABLE IF EXISTS transfers;
DROP TRIGGER IF EXISTS journal_entries_immutable ON journal_entries;
DROP TRIGGER IF EXISTS journal_entries_balanced ON journal_entries;
DROP FUNCTION IF EXISTS forbid_journal_mutation();
DROP FUNCTION IF EXISTS assert_journal_balanced();
DROP TABLE IF EXISTS journal_entries;
DROP TABLE IF EXISTS journal_transactions;
DROP TABLE IF EXISTS account_balances;
DROP TABLE IF EXISTS accounts;
