package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
)

// reacquireTicketCapacity runs inside the lifecycle writer transaction and
// derives both dimensions from the ticket's immutable, original generation.
func reacquireTicketCapacity(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, runner uint64) error {
	project := Project{Channel: ref.Channel, ID: ref.Project}
	if err := conn.QueryRowContext(ctx, `SELECT p.canonical_path,p.base_ref,t.config_generation,t.config_digest,t.config_snapshot_bytes FROM tickets t JOIN projects p ON p.channel=t.channel AND p.id=t.project_id WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&project.Path, &project.BaseRef, &project.ConfigGeneration, &project.ConfigDigest, &project.ConfigSnapshot); err != nil {
		return err
	}
	if project.ConfigGeneration != 0 {
		var digest string
		var snapshot []byte
		if err := conn.QueryRowContext(ctx, `SELECT digest,snapshot_bytes FROM project_configurations WHERE channel=? AND project_id=? AND generation=?`, ref.Channel, ref.Project, project.ConfigGeneration).Scan(&digest, &snapshot); err != nil {
			return ErrProjectConflict
		}
		if digest != project.ConfigDigest || !bytes.Equal(snapshot, project.ConfigSnapshot) {
			return ErrProjectConflict
		}
		frozen, err := config.DecodeSnapshot(snapshot, digest)
		if err != nil || frozen.Name != string(ref.Project) || frozen.Repository != project.Path {
			return ErrProjectConflict
		}
		// The project's currently selected base may have changed along with
		// its latest generation; the resumed ticket still owns the old one.
		project.BaseRef = frozen.BaseBranch
	}
	requests, err := projectStartLeaseRequests(ctx, conn, project, ref)
	if err != nil {
		return err
	}
	for _, request := range requests {
		rows, err := conn.QueryContext(ctx, `SELECT scope_key,runner_epoch,acquired_at FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND scope=?`, ref.Channel, ref.Project, ref.Ticket, request.Scope)
		if err != nil {
			return err
		}
		owned := 0
		valid := true
		for rows.Next() {
			var key string
			var epoch uint64
			var acquiredAt string
			if err := rows.Scan(&key, &epoch, &acquiredAt); err != nil {
				rows.Close()
				return err
			}
			owned++
			matched := false
			for slot := 0; slot < request.Capacity; slot++ {
				matched = matched || key == leaseKey(request.Resource, slot)
			}
			_, stampErr := time.Parse(time.RFC3339Nano, acquiredAt)
			valid = valid && matched && epoch == runner && stampErr == nil
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if !valid || owned > 1 {
			return ErrStaleFence
		}
		if owned == 1 {
			continue
		}
		if _, ok, err := acquireLease(ctx, conn, ref, runner, request, time.Now().UTC()); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("%w: scope=%s resource=%s capacity=%d", ErrLeaseCapacity, request.Scope, request.Resource, request.Capacity)
		}
	}
	return nil
}
