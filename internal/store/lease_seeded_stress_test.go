package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// Exercise Store capacity, not a claim that the production runtime admits four
// workers. Each seed races duplicate acquisitions against two coupled limits,
// checks rollback/idempotence, then proves rejected tickets can reuse all slots.
func TestSeededLeaseAdmissionStress(t *testing.T) {
	for _, capacity := range []int{2, 4} {
		// Reuse the migrated fixture across seeds: schema migration is not the
		// stress target. Distinct ticket identities and empty leases between
		// seeds retain isolation without migrating 100 databases under -race.
		db, parent := openTestStore(t)
		leader, err := db.AcquireLeader(parent, domain.ChannelDev, "seeded-admission")
		if err != nil {
			t.Fatal(err)
		}
		for seed := int64(0); seed < 50; seed++ {
			passed := t.Run(fmt.Sprintf("capacity%d/seed%d", capacity, seed), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(parent, 15*time.Second)
				defer cancel()
				fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}
				refs := make([]domain.TicketRef, capacity*2)
				for i := range refs {
					refs[i] = createLeaseTicket(t, db, int(seed)*capacity*2+i)
				}
				requests := []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: capacity}, {Scope: "project", Resource: "nysa", Capacity: capacity}}
				type outcome struct {
					ref domain.TicketRef
					err error
				}
				results := make(chan outcome, len(refs)*2)
				start := make(chan struct{})
				var workers sync.WaitGroup
				for _, index := range rand.New(rand.NewSource(seed)).Perm(len(refs) * 2) {
					ref := refs[index%len(refs)]
					workers.Add(1)
					go func() {
						defer workers.Done()
						<-start
						_, err := db.AcquireLeases(ctx, ref, 1, fence, requests, time.Now().UTC())
						results <- outcome{ref, err}
					}()
				}
				close(start)
				workers.Wait()
				close(results)
				admitted := map[domain.TicketRef]int{}
				for result := range results {
					if result.err == nil {
						admitted[result.ref]++
					} else if !errors.Is(result.err, ErrLeaseCapacity) {
						t.Fatalf("acquire: %v", result.err)
					}
				}
				if len(admitted) != capacity {
					t.Fatalf("admitted=%v capacity=%d", admitted, capacity)
				}
				for ref, count := range admitted {
					if count != 2 {
						t.Fatalf("duplicate acquisition not idempotent: %v count=%d", ref, count)
					}
				}
				leases, err := db.Leases(ctx, domain.ChannelDev)
				if err != nil || len(leases) != capacity*2 {
					t.Fatalf("partial/extra leases=%+v err=%v", leases, err)
				}
				for _, lease := range leases {
					if admitted[lease.Ref] != 2 {
						t.Fatalf("rejected ticket retained partial lease: %+v", lease)
					}
				}
				for ref := range admitted {
					stale := fence
					stale.LeaderEpoch++
					if _, err := db.ReleaseLeases(ctx, ref, 1, stale); !errors.Is(err, ErrStaleFence) {
						t.Fatalf("wrong leader released lease: %v", err)
					}
					if released, err := db.ReleaseLeases(ctx, ref, 1, fence); err != nil || released != 2 {
						t.Fatalf("release=%d err=%v", released, err)
					}
				}
				for _, ref := range refs {
					if admitted[ref] != 0 {
						continue
					}
					if _, err := db.AcquireLeases(ctx, ref, 1, fence, requests, time.Now().UTC()); err != nil {
						t.Fatalf("queued ticket could not reuse capacity: %v", err)
					}
				}
				for _, ref := range refs {
					if _, err := db.ReleaseLeases(ctx, ref, 1, fence); err != nil {
						t.Fatal(err)
					}
				}
				if leases, err := db.Leases(ctx, domain.ChannelDev); err != nil || len(leases) != 0 {
					t.Fatalf("leaked leases=%+v err=%v", leases, err)
				}
			})
			if !passed {
				return // Preserve the first failing seed; do not cascade its leases.
			}
		}
	}
}
