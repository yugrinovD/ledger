package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	"github.com/yugrinovD/ledger/internal/adapter/postgres"
	"github.com/yugrinovD/ledger/internal/auth"
	"github.com/yugrinovD/ledger/internal/config"
	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/usecase"
	"github.com/yugrinovD/ledger/internal/version"
)

var databaseURL string

func main() {
	root := rootCmd()
	root.SilenceUsage = true
	root.SilenceErrors = true
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "ledgerctl",
		Short: "Operational CLI for the Ledger service",
	}
	root.PersistentFlags().StringVar(&databaseURL, "database-url", "", "override LEDGER_DATABASE_URL")
	root.AddCommand(
		versionCmd(),
		migrateCmd(),
		apiKeyCmd(),
		apiKeyRevokeCmd(),
		accountCmd(),
		transferCmd(),
		holdCmd(),
		reconcileCmd(),
		outboxCmd(),
		seedCmd(),
		debugCmd(),
	)
	return root
}

type appContext struct {
	cfg        config.Config
	store      *postgres.Store
	svc        *usecase.Service
	poolCloser func()
}

func newApp() (*appContext, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	if databaseURL != "" {
		cfg.DatabaseURL = databaseURL
	}
	ctx := context.Background()
	pool, err := postgres.Open(ctx, cfg.DatabaseURL, postgres.PoolConfig{
		MaxConns: 5, MinConns: 1, MaxConnLifetime: time.Hour,
		MaxConnIdleTime: 5 * time.Minute, ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	store := postgres.New(pool)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	idem := idempotency.NewExecutor(pool, store, cfg.IdempotencyTTL, nil, nil)
	svc := usecase.New(store, pool, idem, metrics, logger)
	svc.SetReconciliationTimeout(cfg.ReconciliationStatementTimeout)
	return &appContext{cfg: cfg, store: store, svc: svc, poolCloser: pool.Close}, nil
}

func (a *appContext) Close() {
	if a != nil && a.poolCloser != nil {
		a.poolCloser()
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print build information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("version=%s commit=%s date=%s\n", version.Version, version.Commit, version.Date)
		},
	}
}

func migrateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate [up|down|status]",
		Short: "Apply database migrations",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			if databaseURL != "" {
				cfg.DatabaseURL = databaseURL
			}
			direction := postgres.MigrationDirection(args[0])
			return postgres.Migrate(cmd.Context(), cfg.DatabaseURL, direction)
		},
	}
	return cmd
}

func apiKeyRevokeCmd() *cobra.Command {
	var idRaw string
	cmd := &cobra.Command{
		Use:   "apikey-revoke",
		Short: "Revoke an API key",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			id, err := uuid.Parse(idRaw)
			if err != nil {
				return fmt.Errorf("invalid key id: %w", err)
			}
			if err := app.svc.RevokeAPIKey(cmd.Context(), id); err != nil {
				return err
			}
			fmt.Printf("revoked api key %s\n", id)
			return nil
		},
	}
	cmd.Flags().StringVar(&idRaw, "id", "", "api key id")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

func apiKeyCmd() *cobra.Command {
	var owner, name, scopes string
	cmd := &cobra.Command{
		Use:   "apikey-create",
		Short: "Create an API key (the plaintext is printed once)",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			plain, prefix, err := auth.GenerateKey()
			if err != nil {
				return err
			}
			hash, err := auth.Hash(app.cfg.AuthPepper, plain)
			if err != nil {
				return err
			}
			scopeList := splitScopes(scopes)
			id, err := uuid.NewV7()
			if err != nil {
				return err
			}
			if err := app.store.InsertAPIKey(cmd.Context(), id, owner, name, prefix, hash, scopeList); err != nil {
				return err
			}
			return printJSON(map[string]any{
				"id": id, "owner_id": owner, "name": name, "key": plain, "prefix": prefix, "scopes": scopeList,
				"warning": "store this key now; it cannot be recovered",
			})
		},
	}
	cmd.Flags().StringVar(&owner, "owner", "", "owner id")
	cmd.Flags().StringVar(&name, "name", "default", "key name")
	cmd.Flags().StringVar(&scopes, "scopes", "*", "comma-separated scopes")
	_ = cmd.MarkFlagRequired("owner")
	return cmd
}

func accountCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "account", Short: "Account operations"}
	var owner, name, currency, accountType, key string
	var allowNegative bool
	create := &cobra.Command{
		Use:   "create",
		Short: "Create an account",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			if key == "" {
				key = "cli-account-" + uuid.NewString()
			}
			view, _, err := app.svc.CreateAccount(cmd.Context(), usecase.CreateAccountInput{
				OwnerID: owner, IdempotencyKey: key, Name: name,
				Currency: currency, Type: accountType, AllowNegative: allowNegative,
			})
			if err != nil {
				return err
			}
			return printJSON(view)
		},
	}
	create.Flags().StringVar(&owner, "owner", "", "owner id")
	create.Flags().StringVar(&name, "name", "", "account name")
	create.Flags().StringVar(&currency, "currency", "USD", "ISO currency")
	create.Flags().StringVar(&accountType, "type", "liability", "asset|liability|equity|revenue|expense")
	create.Flags().BoolVar(&allowNegative, "allow-negative", false, "allow negative balance (system accounts)")
	create.Flags().StringVar(&key, "idempotency-key", "", "idempotency key")
	_ = create.MarkFlagRequired("owner")
	_ = create.MarkFlagRequired("name")

	var showOwner string
	var accountID string
	balance := &cobra.Command{
		Use:   "balance",
		Short: "Show an account and its balance",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			id, err := uuid.Parse(accountID)
			if err != nil {
				return fmt.Errorf("invalid account id: %w", err)
			}
			acc, err := app.svc.GetAccount(cmd.Context(), showOwner, id)
			if err != nil {
				return err
			}
			bal, err := app.svc.GetBalance(cmd.Context(), showOwner, id)
			if err != nil {
				return err
			}
			return printJSON(map[string]any{"account": acc, "balance": bal})
		},
	}
	balance.Flags().StringVar(&showOwner, "owner", "", "owner id")
	balance.Flags().StringVar(&accountID, "account", "", "account id")
	_ = balance.MarkFlagRequired("owner")
	_ = balance.MarkFlagRequired("account")
	statusCmd := func(name, short string, action func(context.Context, *appContext, uuid.UUID) (usecase.AccountView, error)) *cobra.Command {
		var account string
		cmd := &cobra.Command{
			Use:   name,
			Short: short,
			RunE: func(cmd *cobra.Command, args []string) error {
				app, err := newApp()
				if err != nil {
					return err
				}
				defer app.Close()
				id, err := uuid.Parse(account)
				if err != nil {
					return fmt.Errorf("invalid account id: %w", err)
				}
				view, err := action(cmd.Context(), app, id)
				if err != nil {
					return err
				}
				return printJSON(view)
			},
		}
		cmd.Flags().StringVar(&account, "account", "", "account id")
		_ = cmd.MarkFlagRequired("account")
		return cmd
	}
	cmd.AddCommand(
		create,
		balance,
		statusCmd("freeze", "Freeze an account", func(ctx context.Context, app *appContext, id uuid.UUID) (usecase.AccountView, error) {
			return app.svc.FreezeAccount(ctx, id)
		}),
		statusCmd("unfreeze", "Unfreeze an account", func(ctx context.Context, app *appContext, id uuid.UUID) (usecase.AccountView, error) {
			return app.svc.UnfreezeAccount(ctx, id)
		}),
		statusCmd("close", "Close an account with zero balance and no holds", func(ctx context.Context, app *appContext, id uuid.UUID) (usecase.AccountView, error) {
			return app.svc.CloseAccount(ctx, id)
		}),
	)
	return cmd
}

func transferCmd() *cobra.Command {
	var owner, from, to, amount, currency, description, key string
	cmd := &cobra.Command{
		Use:   "transfer",
		Short: "Create a transfer between two accounts",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			src, err := uuid.Parse(from)
			if err != nil {
				return fmt.Errorf("invalid source: %w", err)
			}
			dst, err := uuid.Parse(to)
			if err != nil {
				return fmt.Errorf("invalid destination: %w", err)
			}
			if key == "" {
				key = "cli-transfer-" + uuid.NewString()
			}
			view, replayed, err := app.svc.CreateTransfer(cmd.Context(), usecase.CreateTransferInput{
				OwnerID: owner, IdempotencyKey: key, SourceAccountID: src, DestinationAccountID: dst,
				Amount: amount, Currency: currency, Description: description,
			})
			if err != nil {
				return err
			}
			return printJSON(map[string]any{"transfer": view, "replayed": replayed})
		},
	}
	cmd.Flags().StringVar(&owner, "owner", "", "owner id (must own source)")
	cmd.Flags().StringVar(&from, "from", "", "source account id")
	cmd.Flags().StringVar(&to, "to", "", "destination account id")
	cmd.Flags().StringVar(&amount, "amount", "", "amount, e.g. 10.50")
	cmd.Flags().StringVar(&currency, "currency", "USD", "ISO currency")
	cmd.Flags().StringVar(&description, "description", "", "description")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "idempotency key")
	_ = cmd.MarkFlagRequired("owner")
	_ = cmd.MarkFlagRequired("from")
	_ = cmd.MarkFlagRequired("to")
	_ = cmd.MarkFlagRequired("amount")
	return cmd
}

func holdCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "hold", Short: "Hold operations"}
	var owner, account, amount, currency, key, expires string
	create := &cobra.Command{
		Use:   "create",
		Short: "Reserve funds on an account",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			accountID, err := uuid.Parse(account)
			if err != nil {
				return fmt.Errorf("invalid account: %w", err)
			}
			var expiresAt *time.Time
			if expires != "" {
				t, err := time.Parse(time.RFC3339, expires)
				if err != nil {
					return fmt.Errorf("invalid expires: %w", err)
				}
				expiresAt = &t
			}
			if key == "" {
				key = "cli-hold-" + uuid.NewString()
			}
			view, _, err := app.svc.CreateHold(cmd.Context(), usecase.CreateHoldInput{
				OwnerID: owner, IdempotencyKey: key, AccountID: accountID,
				Amount: amount, Currency: currency, ExpiresAt: expiresAt,
			})
			if err != nil {
				return err
			}
			return printJSON(view)
		},
	}
	create.Flags().StringVar(&owner, "owner", "", "owner id")
	create.Flags().StringVar(&account, "account", "", "account id")
	create.Flags().StringVar(&amount, "amount", "", "amount")
	create.Flags().StringVar(&currency, "currency", "USD", "ISO currency")
	create.Flags().StringVar(&key, "idempotency-key", "", "idempotency key")
	create.Flags().StringVar(&expires, "expires", "", "RFC3339 expiry")
	_ = create.MarkFlagRequired("owner")
	_ = create.MarkFlagRequired("account")
	_ = create.MarkFlagRequired("amount")

	var capOwner, capHold, destination, capKey string
	capture := &cobra.Command{
		Use:   "capture",
		Short: "Capture a hold into a destination account",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			holdID, err := uuid.Parse(capHold)
			if err != nil {
				return fmt.Errorf("invalid hold: %w", err)
			}
			dst, err := uuid.Parse(destination)
			if err != nil {
				return fmt.Errorf("invalid destination: %w", err)
			}
			if capKey == "" {
				capKey = "cli-capture-" + uuid.NewString()
			}
			view, _, err := app.svc.CaptureHold(cmd.Context(), usecase.CaptureHoldInput{
				OwnerID: capOwner, IdempotencyKey: capKey, HoldID: holdID, DestinationAccountID: dst,
			})
			if err != nil {
				return err
			}
			return printJSON(view)
		},
	}
	capture.Flags().StringVar(&capOwner, "owner", "", "owner id")
	capture.Flags().StringVar(&capHold, "hold", "", "hold id")
	capture.Flags().StringVar(&destination, "destination", "", "destination account id")
	capture.Flags().StringVar(&capKey, "idempotency-key", "", "idempotency key")
	_ = capture.MarkFlagRequired("owner")
	_ = capture.MarkFlagRequired("hold")
	_ = capture.MarkFlagRequired("destination")

	var relOwner, relHold, relKey string
	release := &cobra.Command{
		Use:   "release",
		Short: "Release a hold",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			holdID, err := uuid.Parse(relHold)
			if err != nil {
				return fmt.Errorf("invalid hold: %w", err)
			}
			if relKey == "" {
				relKey = "cli-release-" + uuid.NewString()
			}
			view, _, err := app.svc.ReleaseHold(cmd.Context(), usecase.ReleaseHoldInput{
				OwnerID: relOwner, IdempotencyKey: relKey, HoldID: holdID,
			})
			if err != nil {
				return err
			}
			return printJSON(view)
		},
	}
	release.Flags().StringVar(&relOwner, "owner", "", "owner id")
	release.Flags().StringVar(&relHold, "hold", "", "hold id")
	release.Flags().StringVar(&relKey, "idempotency-key", "", "idempotency key")
	_ = release.MarkFlagRequired("owner")
	_ = release.MarkFlagRequired("hold")

	cmd.AddCommand(create, capture, release)
	return cmd
}

func reconcileCmd() *cobra.Command {
	var repair bool
	cmd := &cobra.Command{
		Use:   "reconcile",
		Short: "Run accounting reconciliation",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			res, err := app.svc.Reconcile(cmd.Context(), repair)
			if err != nil {
				return err
			}
			return printJSON(map[string]any{
				"run":           res.Run,
				"discrepancies": res.Discrepancies,
				"repair":        repair,
			})
		},
	}
	cmd.Flags().BoolVar(&repair, "repair", false, "repair derived balances from the immutable journal")
	return cmd
}

func outboxCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "outbox", Short: "Outbox operations"}
	status := &cobra.Command{
		Use:   "status",
		Short: "Show outbox backlog",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			pending, oldest, err := app.store.OutboxStats(cmd.Context(), app.store.Pool())
			if err != nil {
				return err
			}
			parked, err := app.store.OutboxParkedCount(cmd.Context(), app.store.Pool())
			if err != nil {
				return err
			}
			return printJSON(map[string]any{"pending": pending, "parked": parked, "oldest_age_seconds": oldest})
		},
	}
	var parkedLimit int
	parked := &cobra.Command{
		Use:   "ls-parked",
		Short: "List parked outbox events",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			events, err := app.svc.ListParkedOutbox(cmd.Context(), parkedLimit)
			if err != nil {
				return err
			}
			return printJSON(map[string]any{"events": events})
		},
	}
	parked.Flags().IntVar(&parkedLimit, "limit", 20, "maximum number of events")
	var requeueID int64
	requeue := &cobra.Command{
		Use:   "requeue",
		Short: "Requeue a parked outbox event",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			if err := app.svc.RequeueOutbox(cmd.Context(), requeueID); err != nil {
				return err
			}
			return printJSON(map[string]any{"requeued": true, "id": requeueID})
		},
	}
	requeue.Flags().Int64Var(&requeueID, "id", 0, "outbox event id")
	_ = requeue.MarkFlagRequired("id")
	cmd.AddCommand(status, parked, requeue)
	return cmd
}

func seedCmd() *cobra.Command {
	var customers int
	var currency, fund, output string
	cmd := &cobra.Command{
		Use:   "seed",
		Short: "Create a system account, customers and initial funding",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			ctx := cmd.Context()
			system, _, err := app.svc.CreateAccount(ctx, usecase.CreateAccountInput{
				OwnerID: "system", IdempotencyKey: "seed:system", Name: "system-equity",
				Currency: currency, Type: string(domain.AccountEquity), AllowNegative: true,
			})
			if err != nil {
				return fmt.Errorf("create system account: %w", err)
			}
			fmt.Printf("system account: %s\n", system.ID)
			seeded := make([]usecase.AccountView, 0, customers)
			for i := 1; i <= customers; i++ {
				name := fmt.Sprintf("customer-%04d", i)
				acc, _, err := app.svc.CreateAccount(ctx, usecase.CreateAccountInput{
					OwnerID: "demo", IdempotencyKey: "seed:account:" + name, Name: name,
					Currency: currency, Type: string(domain.AccountLiability),
				})
				if err != nil {
					return fmt.Errorf("create %s: %w", name, err)
				}
				if fund != "" {
					if _, _, err := app.svc.CreateTransfer(ctx, usecase.CreateTransferInput{
						OwnerID: "system", IdempotencyKey: "seed:fund:" + name,
						SourceAccountID: uuid.MustParse(system.ID), DestinationAccountID: uuid.MustParse(acc.ID),
						Amount: fund, Currency: currency, Description: "seed funding",
					}); err != nil {
						return fmt.Errorf("fund %s: %w", name, err)
					}
				}
				seeded = append(seeded, acc)
				if i%100 == 0 || i == customers {
					fmt.Printf("seeded %d/%d customers\n", i, customers)
				}
			}
			if output != "" {
				raw, err := json.MarshalIndent(map[string]any{
					"currency": currency,
					"system":   system,
					"accounts": seeded,
				}, "", "  ")
				if err != nil {
					return err
				}
				if err := os.WriteFile(output, raw, 0o644); err != nil {
					return fmt.Errorf("write seed output: %w", err)
				}
				fmt.Printf("accounts written to %s\n", output)
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&customers, "customers", 10, "number of customer accounts")
	cmd.Flags().StringVar(&currency, "currency", "USD", "ISO currency")
	cmd.Flags().StringVar(&fund, "fund", "1000.00", "initial funding per customer (empty to skip)")
	cmd.Flags().StringVar(&output, "output", "", "write seeded account ids to a JSON file")
	return cmd
}

func debugCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "debug", Short: "Destructive helpers for local demos"}
	var accountID string
	var delta int64
	corrupt := &cobra.Command{
		Use:   "corrupt-balance",
		Short: "Corrupt a projected balance to demonstrate reconciliation (local only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := newApp()
			if err != nil {
				return err
			}
			defer app.Close()
			id, err := uuid.Parse(accountID)
			if err != nil {
				return fmt.Errorf("invalid account: %w", err)
			}
			var before, after int64
			if err := app.store.Pool().QueryRow(cmd.Context(),
				`SELECT balance_minor FROM account_balances WHERE account_id = $1`, id).Scan(&before); err != nil {
				return err
			}
			if err := app.store.Pool().QueryRow(cmd.Context(), `
				UPDATE account_balances
				SET balance_minor = balance_minor + $2, version = version + 1, updated_at = now()
				WHERE account_id = $1
				RETURNING balance_minor`, id, delta).Scan(&after); err != nil {
				return err
			}
			return printJSON(map[string]any{
				"account_id": id, "before_minor": before, "after_minor": after, "delta_minor": delta,
				"warning": "this intentionally breaks the projection; run ledgerctl reconcile to detect it",
			})
		},
	}
	corrupt.Flags().StringVar(&accountID, "account", "", "account id")
	corrupt.Flags().Int64Var(&delta, "delta-minor", 100, "amount in minor units")
	_ = corrupt.MarkFlagRequired("account")
	cmd.AddCommand(corrupt)
	return cmd
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func splitScopes(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
