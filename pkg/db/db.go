// Package db holds the Postgres/Timescale access layer shared by processor and API.
package db

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hareshgoyal06/andon/pkg/events"
)

//go:embed migrations.sql
var migrations string

// DB wraps a pgx pool.
type DB struct {
	Pool *pgxpool.Pool
}

// Connect opens a pool, retrying until the database is reachable.
func Connect(ctx context.Context, log *slog.Logger, dsn string) (*DB, error) {
	backoff := time.Second
	for {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return &DB{Pool: pool}, nil
			}
			pool.Close()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		log.Warn("database not ready", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

// Migrate applies the embedded schema. Idempotent.
func (d *DB) Migrate(ctx context.Context) error {
	_, err := d.Pool.Exec(ctx, migrations)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// Close the pool.
func (d *DB) Close() { d.Pool.Close() }

// InsertEvent is idempotent on (station_id, ts, type) so redelivered Kafka
// messages after a consumer failover do not produce duplicates.
func (d *DB) InsertEvent(ctx context.Context, e events.StationEvent) error {
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO events (ts, station_id, type, state, prev_state, cycle_ms, target_cycle_ms,
		                    part_count, scrap_count, temp_c, fault_code, error)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (station_id, ts, type) DO NOTHING`,
		e.Timestamp, e.StationID, string(e.Type), nz(e.State), nz(e.PrevState),
		int(e.CycleMs), int(e.TargetCycleMs), int(e.PartCount), int(e.ScrapCount),
		e.TempC, int(e.FaultCode), nz(e.Error))
	return err
}

// StationState is the latest known state of a station.
type StationState struct {
	StationID     string    `json:"station_id"`
	State         string    `json:"state"`
	Online        bool      `json:"online"`
	CycleMs       int       `json:"cycle_ms"`
	TargetCycleMs int       `json:"target_cycle_ms"`
	PartCount     int       `json:"part_count"`
	ScrapCount    int       `json:"scrap_count"`
	TempC         float64   `json:"temp_c"`
	FaultCode     int       `json:"fault_code"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// UpsertStationState writes the current state.
func (d *DB) UpsertStationState(ctx context.Context, s StationState) error {
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO station_state (station_id, state, online, cycle_ms, target_cycle_ms, part_count,
		                           scrap_count, temp_c, fault_code, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (station_id) DO UPDATE SET
		  state=EXCLUDED.state, online=EXCLUDED.online, cycle_ms=EXCLUDED.cycle_ms,
		  target_cycle_ms=EXCLUDED.target_cycle_ms, part_count=EXCLUDED.part_count,
		  scrap_count=EXCLUDED.scrap_count, temp_c=EXCLUDED.temp_c, fault_code=EXCLUDED.fault_code,
		  updated_at=EXCLUDED.updated_at
		WHERE station_state.updated_at <= EXCLUDED.updated_at`,
		s.StationID, s.State, s.Online, s.CycleMs, s.TargetCycleMs, s.PartCount, s.ScrapCount,
		s.TempC, s.FaultCode, s.UpdatedAt)
	return err
}

// ListStationState returns all stations ordered by id.
func (d *DB) ListStationState(ctx context.Context) ([]StationState, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT station_id, state, online, COALESCE(cycle_ms,0), COALESCE(target_cycle_ms,0),
		       COALESCE(part_count,0), COALESCE(scrap_count,0), COALESCE(temp_c,0),
		       COALESCE(fault_code,0), updated_at
		FROM station_state ORDER BY station_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StationState
	for rows.Next() {
		var s StationState
		if err := rows.Scan(&s.StationID, &s.State, &s.Online, &s.CycleMs, &s.TargetCycleMs,
			&s.PartCount, &s.ScrapCount, &s.TempC, &s.FaultCode, &s.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// RecentEvents returns the newest events, optionally filtered by station.
func (d *DB) RecentEvents(ctx context.Context, stationID string, since time.Time, limit int) ([]events.StationEvent, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT ts, station_id, type, COALESCE(state,''), COALESCE(prev_state,''), COALESCE(cycle_ms,0),
		       COALESCE(target_cycle_ms,0), COALESCE(part_count,0), COALESCE(scrap_count,0),
		       COALESCE(temp_c,0), COALESCE(fault_code,0), COALESCE(error,'')
		FROM events
		WHERE ($1 = '' OR station_id = $1) AND ts >= $2
		ORDER BY ts DESC LIMIT $3`, stationID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.StationEvent
	for rows.Next() {
		var e events.StationEvent
		var typ string
		var cycle, target, parts, scrap, fault int
		if err := rows.Scan(&e.Timestamp, &e.StationID, &typ, &e.State, &e.PrevState, &cycle,
			&target, &parts, &scrap, &e.TempC, &fault, &e.Error); err != nil {
			return nil, err
		}
		e.Type = events.Type(typ)
		e.CycleMs, e.TargetCycleMs, e.PartCount, e.ScrapCount, e.FaultCode =
			uint16(cycle), uint16(target), uint16(parts), uint16(scrap), uint16(fault)
		out = append(out, e)
	}
	return out, rows.Err()
}

// InsertAlert is idempotent on id.
func (d *DB) InsertAlert(ctx context.Context, a events.Alert) error {
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO alerts (id, ts, station_id, severity, kind, message)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO NOTHING`,
		a.ID, a.Timestamp, a.StationID, string(a.Severity), a.Kind, a.Message)
	return err
}

// RecentAlerts returns the newest alerts.
func (d *DB) RecentAlerts(ctx context.Context, limit int) ([]events.Alert, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT id, ts, station_id, severity, kind, message FROM alerts ORDER BY ts DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.Alert
	for rows.Next() {
		var a events.Alert
		var sev string
		if err := rows.Scan(&a.ID, &a.Timestamp, &a.StationID, &sev, &a.Kind, &a.Message); err != nil {
			return nil, err
		}
		a.Severity = events.Severity(sev)
		out = append(out, a)
	}
	return out, rows.Err()
}

// InsertOEE stores a snapshot.
func (d *DB) InsertOEE(ctx context.Context, o events.OEE) error {
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO oee_snapshots (ts, station_id, availability, performance, quality, oee,
		                           avg_cycle_ms, parts, scrap, faults, is_bottleneck)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING`,
		o.Timestamp, o.StationID, o.Availability, o.Performance, o.Quality, o.OEE,
		o.AvgCycleMs, o.Parts, o.Scrap, o.Faults, o.IsBottleneck)
	return err
}

// LatestOEE returns the most recent snapshot per station.
func (d *DB) LatestOEE(ctx context.Context) ([]events.OEE, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT DISTINCT ON (station_id) ts, station_id, availability, performance, quality, oee,
		       COALESCE(avg_cycle_ms,0), COALESCE(parts,0), COALESCE(scrap,0), COALESCE(faults,0), is_bottleneck
		FROM oee_snapshots WHERE ts > now() - interval '2 minutes'
		ORDER BY station_id, ts DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.OEE
	for rows.Next() {
		var o events.OEE
		if err := rows.Scan(&o.Timestamp, &o.StationID, &o.Availability, &o.Performance, &o.Quality,
			&o.OEE, &o.AvgCycleMs, &o.Parts, &o.Scrap, &o.Faults, &o.IsBottleneck); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// OEEHistory returns snapshots for a station over a period, newest last.
func (d *DB) OEEHistory(ctx context.Context, stationID string, since time.Time) ([]events.OEE, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT ts, station_id, availability, performance, quality, oee,
		       COALESCE(avg_cycle_ms,0), COALESCE(parts,0), COALESCE(scrap,0), COALESCE(faults,0), is_bottleneck
		FROM oee_snapshots WHERE station_id=$1 AND ts >= $2 ORDER BY ts ASC`, stationID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.OEE
	for rows.Next() {
		var o events.OEE
		if err := rows.Scan(&o.Timestamp, &o.StationID, &o.Availability, &o.Performance, &o.Quality,
			&o.OEE, &o.AvgCycleMs, &o.Parts, &o.Scrap, &o.Faults, &o.IsBottleneck); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// CommandRecord is the audit row for an operator command.
type CommandRecord struct {
	events.Command
	Status    string     `json:"status"`
	Error     string     `json:"error,omitempty"`
	AppliedAt *time.Time `json:"applied_at,omitempty"`
}

// InsertCommand records a queued command.
func (d *DB) InsertCommand(ctx context.Context, c events.Command) error {
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO commands (id, ts, station_id, type, value, actor, status)
		VALUES ($1,$2,$3,$4,$5,$6,'queued') ON CONFLICT (id) DO NOTHING`,
		c.ID, c.Timestamp, c.StationID, string(c.Type), int(c.Value), nz(c.Actor))
	return err
}

// MarkCommand updates a command's outcome.
func (d *DB) MarkCommand(ctx context.Context, id, status, errMsg string) error {
	_, err := d.Pool.Exec(ctx, `
		UPDATE commands SET status=$2, error=$3, applied_at=now() WHERE id=$1`, id, status, nz(errMsg))
	return err
}

// RecentCommands lists the newest commands.
func (d *DB) RecentCommands(ctx context.Context, limit int) ([]CommandRecord, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT id, ts, station_id, type, COALESCE(value,0), COALESCE(actor,''), status, COALESCE(error,''), applied_at
		FROM commands ORDER BY ts DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CommandRecord
	for rows.Next() {
		var c CommandRecord
		var typ string
		var val int
		if err := rows.Scan(&c.ID, &c.Timestamp, &c.StationID, &typ, &val, &c.Actor, &c.Status, &c.Error, &c.AppliedAt); err != nil {
			return nil, err
		}
		c.Type = events.CommandType(typ)
		c.Value = uint16(val)
		out = append(out, c)
	}
	return out, rows.Err()
}

func nz(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
