//go:build integration

package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// These tests decide whether per-account ordering is *actually* guaranteed when
// more than one consumer runs. They deliberately do not look at the ids a Claim
// returns: they record the order in which handlers FINISH, because that is the
// order the business sees.

// TestOutboxClaimOrderingSurvivesBlockedHead blocks the handler of the first event
// of one account and watches what a second consumer is allowed to do meanwhile.
//
// With account affinity enforced by the claim, the second consumer must not take
// any later event of the same account while the head is owned, and the recorded
// completion order stays 0..9. Without affinity it will run 1..9 first and the
// completion order becomes 1,2,...,9,0 -- a reordered replay of one account's
// changes.
func TestOutboxClaimOrderingSurvivesBlockedHead(t *testing.T) {
	ctx := context.Background()
	q := claimQueue(t)

	const account = 7401
	const events = 10
	for seq := 0; seq < events; seq++ {
		_, err := integrationDB.ExecContext(ctx, `
			INSERT INTO scheduler_outbox (event_type, account_id, payload)
			VALUES ('account_changed', $1, jsonb_build_object('seq', $2::int))`, account, seq)
		require.NoError(t, err)
	}

	var (
		mu         sync.Mutex
		completed  []int // business completion order, by seeded seq
		claimedByB []int // what the second consumer was allowed to take
		blockHead  = make(chan struct{})
		headTaken  = make(chan struct{})
		headOnce   sync.Once
	)
	handler := func(worker string, seq int) {
		if seq == 0 {
			// The head is deliberately slow: ordered processing means everybody
			// waits, unordered processing means everybody overtakes it.
			headOnce.Do(func() { close(headTaken) })
			<-blockHead
		}
		mu.Lock()
		completed = append(completed, seq)
		mu.Unlock()
	}

	// consumer A: takes the head (batch of 1 so it holds exactly one event).
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		got, err := q.Claim(ctx, "consumer-A", 1, 30*time.Second)
		if err != nil || len(got) != 1 {
			t.Errorf("A claim: err=%v n=%d", err, len(got))
			return
		}
		require.Equal(t, 0, claimSeq(got[0].Payload))
		handler("A", 0)
		if err := q.DeleteClaimed(ctx, got[0].ID, "consumer-A"); err != nil {
			t.Errorf("A ack: %v", err)
		}
	}()

	// consumer B: keeps claiming while the head is blocked.
	bStop := make(chan struct{})
	bDone := make(chan struct{})
	go func() {
		defer close(bDone)
		for {
			select {
			case <-bStop:
				return
			default:
			}
			got, err := q.Claim(ctx, "consumer-B", 10, 30*time.Second)
			if err != nil {
				t.Errorf("B claim: %v", err)
				return
			}
			if len(got) == 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			for _, event := range got {
				seq := claimSeq(event.Payload)
				mu.Lock()
				claimedByB = append(claimedByB, seq)
				mu.Unlock()
				handler("B", seq)
				if err := q.DeleteClaimed(ctx, event.ID, "consumer-B"); err != nil {
					// Losing the claim is legitimate once the lease expired; it must
					// never delete the row silently.
					t.Logf("B ack for seq=%d rejected: %v", seq, err)
				}
			}
		}
	}()

	// Give B plenty of time to overtake while A is blocked.
	select {
	case <-headTaken:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer A never claimed the head")
	}
	time.Sleep(2 * time.Second)
	mu.Lock()
	overtook := append([]int(nil), claimedByB...)
	mu.Unlock()
	close(blockHead) // release the head

	// let both consumers finish, then stop B
	<-aDone
	time.Sleep(1500 * time.Millisecond)
	close(bStop)
	<-bDone
	// drain anything B left behind so the final assertion sees a finished queue
	claimDrain(t, q, "consumer-C")

	mu.Lock()
	order := append([]int(nil), completed...)
	mu.Unlock()

	t.Logf("second consumer claimed while the head was blocked: %v", overtook)
	t.Logf("business completion order: %v", order)

	require.Emptyf(t, overtook,
		"the second consumer overtook the blocked head of the same account: %v -- per-account ordering is not guaranteed across consumers", overtook)
	require.Len(t, order, events)
	for i, seq := range order {
		require.Equalf(t, i, seq, "completion order %v is not strictly 0..%d", order, events-1)
	}
	require.Equal(t, 0, claimCount(t))
}

// TestOutboxClaimStaleOwnerHasNoSideEffectsAfterTakeover lets a lease expire mid
// handling and checks that the previous owner can no longer change anything: its
// ack is rejected, and the row survives for the new owner instead of being deleted
// twice.
func TestOutboxClaimStaleOwnerHasNoSideEffectsAfterTakeover(t *testing.T) {
	ctx := context.Background()
	q := claimQueue(t)
	ids := claimInsert(t, 7402, 1)
	lease := time.Second

	old, err := q.Claim(ctx, "stale-owner", 1, lease)
	require.NoError(t, err)
	require.Len(t, old, 1)

	// The handler outlives the lease.
	time.Sleep(lease + 500*time.Millisecond)

	fresh, err := q.Claim(ctx, "new-owner", 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, fresh, 1, "an expired claim was not handed to the new owner")
	require.Equal(t, ids[0], fresh[0].ID)

	// The stale owner finishes and tries to acknowledge: it must be refused, and
	// must not be able to remove the row the new owner is working on.
	require.Error(t, q.DeleteClaimed(ctx, ids[0], "stale-owner"),
		"a stale owner acknowledged a row it no longer holds")
	require.Equal(t, 1, claimCount(t), "the stale ack removed the row anyway")

	// The new owner completes normally.
	require.NoError(t, q.DeleteClaimed(ctx, ids[0], "new-owner"))
	require.Equal(t, 0, claimCount(t))
	t.Logf("stale owner refused after %s lease, row deleted exactly once by the new owner", lease)
}