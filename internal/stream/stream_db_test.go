package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/derive"
	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/pipeline"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/testdb"
)

func TestWatermarkProgressIdleAndUnderLoad(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 10 * time.Millisecond,
		LeaseTTL:        time.Second,
		Owner:           fmt.Sprintf("load-test-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- watermarker.Run(runCtx) }()

	idleSeq := insertStreamEvent(
		t,
		ctx,
		pool,
		fmt.Sprintf("m5-idle-%d", time.Now().UnixNano()),
	)
	waitSafeSequence(t, pool, idleSeq)

	var maximum atomic.Int64
	var writers sync.WaitGroup
	for worker := range 4 {
		writers.Add(1)
		go func(worker int) {
			defer writers.Done()
			for index := range 25 {
				seq, err := insertStreamEventWithKey(
					ctx,
					pool,
					fmt.Sprintf("m5-load-%d", worker),
					fmt.Sprintf("%d", index),
				)
				if err != nil {
					t.Errorf("insert load event: %v", err)
					return
				}
				for {
					prior := maximum.Load()
					if seq <= prior || maximum.CompareAndSwap(prior, seq) {
						break
					}
				}
			}
		}(worker)
	}
	writers.Wait()
	if target := maximum.Load(); target <= idleSeq {
		t.Fatalf("load maximum seq = %d, idle seq = %d", target, idleSeq)
	} else {
		waitSafeSequence(t, pool, target)
	}
	stop()
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

func TestWatermarkerStandbyFailsOverWithinLeaseTTL(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const leaseTTL = 600 * time.Millisecond
	leader, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 50 * time.Millisecond,
		LeaseTTL:        leaseTTL,
		Owner:           fmt.Sprintf("chaos-leader-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Step(ctx); err != nil {
		t.Fatal(err)
	}

	standby, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 50 * time.Millisecond,
		LeaseTTL:        leaseTTL,
		Owner:           fmt.Sprintf("chaos-standby-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := standby.Step(ctx); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("standby Step error = %v, want ErrLeaseHeld", err)
	}

	target := insertStreamEvent(
		t,
		ctx,
		pool,
		fmt.Sprintf("leader-failover-%d", time.Now().UnixNano()),
	)
	startedAt := time.Now()
	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- standby.Run(runCtx) }()
	waitSafeSequence(t, pool, target)
	if elapsed := time.Since(startedAt); elapsed > leaseTTL+150*time.Millisecond {
		t.Fatalf(
			"standby failover took %s, want within lease TTL %s",
			elapsed,
			leaseTTL,
		)
	}
	stop()
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

func TestTerminatingWatermarkerFenceBackendUnblocksWriterWithoutRegression(
	t *testing.T,
) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := insertStreamEvent(
		t,
		ctx,
		pool,
		fmt.Sprintf("watermarker-kill-%d", time.Now().UnixNano()),
	)
	var priorSafe int64
	if err := pool.QueryRow(ctx, `
		SELECT safe_seq FROM stream_watermark WHERE singleton
	`).Scan(&priorSafe); err != nil {
		t.Fatal(err)
	}

	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval:  20 * time.Millisecond,
		LeaseTTL:         time.Second,
		FenceLockTimeout: time.Second,
		Owner: fmt.Sprintf(
			"exclusive-fence-kill-%d",
			time.Now().UnixNano(),
		),
		InstallationID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeCommit := make(chan struct{})
	releaseCommit := make(chan struct{})
	defer func() {
		select {
		case <-releaseCommit:
		default:
			close(releaseCommit)
		}
	}()
	watermarker.testBeforeCommit = func() {
		close(beforeCommit)
		<-releaseCommit
	}
	stepErr := make(chan error, 1)
	go func() {
		_, err := watermarker.Step(ctx)
		stepErr <- err
	}()

	select {
	case <-beforeCommit:
	case <-ctx.Done():
		t.Fatalf("watermarker did not reach commit gate: %v", ctx.Err())
	}
	watermarkerPID := waitForExclusiveFenceHolder(t, ctx, pool)
	type writerResult struct {
		seq int64
		err error
	}
	writerDone := make(chan writerResult, 1)
	go func() {
		seq, err := insertStreamEventWithKey(
			ctx,
			pool,
			"watermarker-kill-writer",
			"unblocked",
		)
		writerDone <- writerResult{seq: seq, err: err}
	}()
	waitForFenceLocks(t, ctx, pool, "ShareLock", false, 1)

	var terminated bool
	if err := pool.QueryRow(
		ctx,
		`SELECT pg_terminate_backend($1)`,
		watermarkerPID,
	).Scan(&terminated); err != nil {
		t.Fatal(err)
	}
	if !terminated {
		t.Fatalf("watermarker backend %d was not terminated", watermarkerPID)
	}
	close(releaseCommit)
	if err := <-stepErr; err == nil {
		t.Fatal("terminated watermarker step unexpectedly succeeded")
	}
	writer := <-writerDone
	if writer.err != nil {
		t.Fatalf("writer remained blocked after backend termination: %v", writer.err)
	}

	var afterTermination int64
	if err := pool.QueryRow(ctx, `
		SELECT safe_seq FROM stream_watermark WHERE singleton
	`).Scan(&afterTermination); err != nil {
		t.Fatal(err)
	}
	if afterTermination < priorSafe {
		t.Fatalf(
			"safe_seq regressed from %d to %d",
			priorSafe,
			afterTermination,
		)
	}
	watermarker.testBeforeCommit = nil
	progress, err := watermarker.Step(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if progress.SafeSeq < target || progress.SafeSeq < writer.seq {
		t.Fatalf(
			"recovered safe_seq = %d, want at least original %d and writer %d",
			progress.SafeSeq,
			target,
			writer.seq,
		)
	}
}

func TestWatermarkWaitsForRegisteredWriterTransaction(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, writer); err != nil {
		t.Fatal(err)
	}
	var seq int64
	if err := writer.QueryRow(ctx, `
		INSERT INTO change_events (stream, kind, entity_key, payload)
		VALUES ('writer-wait', 'test.changed', 'writer', '{"version":1}')
		RETURNING seq
	`).Scan(&seq); err != nil {
		t.Fatal(err)
	}

	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 10 * time.Millisecond,
		LeaseTTL:        time.Second,
		Owner:           fmt.Sprintf("writer-wait-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	beforeFence := make(chan struct{})
	watermarker.testBeforeFence = func() { close(beforeFence) }
	progress := make(chan WatermarkProgress, 1)
	stepErr := make(chan error, 1)
	go func() {
		got, err := watermarker.Step(ctx)
		progress <- got
		stepErr <- err
	}()
	<-beforeFence
	select {
	case err := <-stepErr:
		t.Fatalf("watermark did not wait for writer transaction: %v", err)
	default:
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-stepErr; err != nil {
		t.Fatal(err)
	}
	if got := <-progress; got.SafeSeq < seq {
		t.Fatalf("safe seq = %d, want at least %d", got.SafeSeq, seq)
	}
}

func TestWatermarkerHeartbeatDoesNotExtendWriterConvoyOrPublishLateGap(
	t *testing.T,
) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, `
		INSERT INTO operation_heartbeats (
		    installation_id, component, operation, success_count,
		    sample_count, last_success_at
		)
		VALUES (1, 'watermarker', 'entities', 0, 0, clock_timestamp())
	`); err != nil {
		t.Fatal(err)
	}
	heartbeatLock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer heartbeatLock.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if _, err := heartbeatLock.Exec(ctx, `
		UPDATE operation_heartbeats
		SET success_count = success_count
		WHERE installation_id = 1
		  AND component = 'watermarker'
		  AND operation = 'entities'
	`); err != nil {
		t.Fatal(err)
	}
	var heartbeatLockPID int32
	if err := heartbeatLock.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(
		&heartbeatLockPID,
	); err != nil {
		t.Fatal(err)
	}

	leadingWriter, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer leadingWriter.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, leadingWriter); err != nil {
		t.Fatal(err)
	}
	leadingSeq, err := insertStreamEventTx(
		ctx,
		leadingWriter,
		"watermark-convoy",
		"leading",
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}

	watermarkObserver := &convoyWatermarkObserver{
		fences: make(chan outbox.FenceObservation, 1),
	}
	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 10 * time.Millisecond,
		LeaseTTL:        time.Second,
		Owner: fmt.Sprintf(
			"watermark-convoy-%d",
			time.Now().UnixNano(),
		),
		Observer:       watermarkObserver,
		InstallationID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	beforeFence := make(chan struct{})
	watermarker.testBeforeFence = func() { close(beforeFence) }
	type stepResult struct {
		progress WatermarkProgress
		err      error
	}
	stepDone := make(chan stepResult, 1)
	go func() {
		progress, err := watermarker.Step(ctx)
		stepDone <- stepResult{progress: progress, err: err}
	}()
	select {
	case <-beforeFence:
	case <-ctx.Done():
		t.Fatalf("watermarker did not reach fence: %v", ctx.Err())
	}
	waitForFenceLocks(t, ctx, pool, "ExclusiveLock", false, 1)

	lateWriter, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lateWriter.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	writerObserver := &fenceObservationRecorder{
		observations: make(chan outbox.FenceObservation, 1),
	}
	type writerResult struct {
		seq int64
		err error
	}
	lateAllocated := make(chan int64, 1)
	releaseLateWriter := make(chan struct{})
	defer func() {
		select {
		case <-releaseLateWriter:
		default:
			close(releaseLateWriter)
		}
	}()
	lateWriterDone := make(chan writerResult, 1)
	go func() {
		observedTx, err := outbox.AcquireObservedWriterFence(
			ctx,
			lateWriter,
			writerObserver,
		)
		if err != nil {
			lateWriterDone <- writerResult{err: err}
			return
		}
		seq, err := insertStreamEventTx(
			ctx,
			observedTx,
			"watermark-convoy",
			"late-low",
			time.Now(),
		)
		if err == nil {
			lateAllocated <- seq
			<-releaseLateWriter
			err = observedTx.Commit(ctx)
		}
		lateWriterDone <- writerResult{seq: seq, err: err}
	}()
	waitForFenceLocks(t, ctx, pool, "ShareLock", false, 1)

	if err := leadingWriter.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	waitForBackendBlockedBy(t, ctx, pool, heartbeatLockPID)
	var lateSeq int64
	select {
	case lateSeq = <-lateAllocated:
	case <-ctx.Done():
		t.Fatalf("late writer did not allocate during heartbeat: %v", ctx.Err())
	}
	highSeq, err := insertStreamEventWithKey(
		ctx,
		pool,
		"watermark-convoy",
		"late-high",
	)
	if err != nil {
		t.Fatal(err)
	}
	if highSeq <= lateSeq {
		t.Fatalf(
			"late sequences low=%d high=%d, want low < high",
			lateSeq,
			highSeq,
		)
	}
	if held := fenceLockCount(
		t,
		ctx,
		pool,
		"ExclusiveLock",
		true,
	); held != 0 {
		t.Fatalf(
			"watermarker retained %d exclusive fences while heartbeat waited",
			held,
		)
	}
	select {
	case result := <-stepDone:
		t.Fatalf(
			"watermark step finished before blocked heartbeat was released: %+v",
			result,
		)
	default:
	}
	var published int64
	if err := pool.QueryRow(ctx, `
		SELECT safe_seq FROM stream_watermark WHERE singleton
	`).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published < leadingSeq {
		t.Fatalf(
			"safe seq = %d, want at least committed leading writer %d",
			published,
			leadingSeq,
		)
	}
	if published >= lateSeq {
		t.Fatalf(
			"safe seq = %d crossed uncommitted late writer %d below committed %d",
			published,
			lateSeq,
			highSeq,
		)
	}
	assertFenceObservation(
		t,
		ctx,
		watermarkObserver.fences,
		outbox.ExclusiveWatermarkFence,
	)

	if err := heartbeatLock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var step stepResult
	select {
	case step = <-stepDone:
	case <-ctx.Done():
		t.Fatalf("watermark step did not finish: %v", ctx.Err())
	}
	if step.err != nil {
		t.Fatal(step.err)
	}
	if step.progress.SafeSeq < leadingSeq {
		t.Fatalf(
			"safe seq = %d, want at least committed leading writer %d",
			step.progress.SafeSeq,
			leadingSeq,
		)
	}
	if step.progress.SafeSeq >= lateSeq {
		t.Fatalf(
			"safe seq = %d crossed uncommitted late writer %d below committed %d",
			step.progress.SafeSeq,
			lateSeq,
			highSeq,
		)
	}
	close(releaseLateWriter)
	var late writerResult
	select {
	case late = <-lateWriterDone:
	case <-ctx.Done():
		t.Fatalf("late writer did not commit: %v", ctx.Err())
	}
	if late.err != nil {
		t.Fatalf("late writer: %v", late.err)
	}
	if late.seq != lateSeq {
		t.Fatalf("late writer seq = %d, want %d", late.seq, lateSeq)
	}
	assertFenceObservation(
		t,
		ctx,
		writerObserver.observations,
		outbox.SharedWriterFence,
	)
	watermarker.testBeforeFence = nil
	followup, err := watermarker.Step(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if followup.SafeSeq < highSeq {
		t.Fatalf(
			"follow-up safe seq = %d, want late writers through %d",
			followup.SafeSeq,
			highSeq,
		)
	}
}

func TestWatermarkerHeartbeatFailurePreservesCommittedProgressAndFenceRelease(
	t *testing.T,
) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, `
		INSERT INTO operation_heartbeats (
		    installation_id, component, operation, success_count,
		    sample_count, last_success_at
		)
		VALUES (1, 'watermarker', 'entities', 0, 0, clock_timestamp())
	`); err != nil {
		t.Fatal(err)
	}
	target := insertStreamEvent(
		t,
		ctx,
		pool,
		fmt.Sprintf("watermark-heartbeat-failure-%d", time.Now().UnixNano()),
	)
	heartbeatLock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer heartbeatLock.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if _, err := heartbeatLock.Exec(ctx, `
		UPDATE operation_heartbeats
		SET success_count = success_count
		WHERE installation_id = 1
		  AND component = 'watermarker'
		  AND operation = 'entities'
	`); err != nil {
		t.Fatal(err)
	}
	var heartbeatLockPID int32
	if err := heartbeatLock.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(
		&heartbeatLockPID,
	); err != nil {
		t.Fatal(err)
	}

	observer := &watermarkProgressRecorder{
		steps: make(chan WatermarkProgress, 1),
	}
	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 10 * time.Millisecond,
		LeaseTTL:        time.Second,
		Owner: fmt.Sprintf(
			"watermark-heartbeat-failure-%d",
			time.Now().UnixNano(),
		),
		Observer:       observer,
		InstallationID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	stepCtx, cancelStep := context.WithCancel(ctx)
	defer cancelStep()
	type stepResult struct {
		progress WatermarkProgress
		err      error
	}
	stepDone := make(chan stepResult, 1)
	go func() {
		progress, err := watermarker.Step(stepCtx)
		stepDone <- stepResult{progress: progress, err: err}
	}()
	waitForBackendBlockedBy(t, ctx, pool, heartbeatLockPID)

	var observed WatermarkProgress
	select {
	case observed = <-observer.steps:
	case <-ctx.Done():
		t.Fatalf("committed watermark was not observed: %v", ctx.Err())
	}
	if observed.SafeSeq < target || !observed.Advanced {
		t.Fatalf(
			"observed committed progress = %+v, want advance through %d",
			observed,
			target,
		)
	}
	var published int64
	if err := pool.QueryRow(ctx, `
		SELECT safe_seq FROM stream_watermark WHERE singleton
	`).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published != observed.SafeSeq {
		t.Fatalf(
			"published safe seq = %d, observed %d",
			published,
			observed.SafeSeq,
		)
	}
	if held := fenceLockCount(
		t,
		ctx,
		pool,
		"ExclusiveLock",
		true,
	); held != 0 {
		t.Fatalf(
			"watermarker retained %d exclusive fences after publication",
			held,
		)
	}
	writerSeq, err := insertStreamEventWithKey(
		ctx,
		pool,
		"watermark-heartbeat-failure",
		"writer-after-publish",
	)
	if err != nil {
		t.Fatalf("writer blocked behind failed heartbeat: %v", err)
	}

	cancelStep()
	var result stepResult
	select {
	case result = <-stepDone:
	case <-ctx.Done():
		t.Fatalf("canceled heartbeat did not return: %v", ctx.Err())
	}
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("heartbeat failure = %v, want context cancellation", result.err)
	}
	if result.progress != observed {
		t.Fatalf(
			"returned progress = %+v, observed committed progress %+v",
			result.progress,
			observed,
		)
	}
	if err := heartbeatLock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	followup, err := watermarker.Step(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if followup.SafeSeq < writerSeq {
		t.Fatalf(
			"follow-up safe seq = %d, want writer %d",
			followup.SafeSeq,
			writerSeq,
		)
	}
}

func TestWatermarkerBoundsFenceWaitRetriesAndAdvancesAfterWriterTermination(
	t *testing.T,
) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := insertStreamEvent(
		t,
		ctx,
		pool,
		fmt.Sprintf("bounded-fence-%d", time.Now().UnixNano()),
	)
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, writer); err != nil {
		t.Fatal(err)
	}
	var writerPID int32
	if err := writer.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(
		&writerPID,
	); err != nil {
		t.Fatal(err)
	}

	observer := &watermarkProgressRecorder{
		steps: make(chan WatermarkProgress, 16),
	}
	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval:  20 * time.Millisecond,
		LeaseTTL:         time.Second,
		FenceLockTimeout: 40 * time.Millisecond,
		Owner: fmt.Sprintf(
			"bounded-fence-%d",
			time.Now().UnixNano(),
		),
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runErr := make(chan error, 1)
	startedAt := time.Now()
	go func() { runErr <- watermarker.Run(runCtx) }()

	for range 2 {
		select {
		case progress := <-observer.steps:
			if !progress.FenceTimedOut {
				t.Fatalf(
					"blocked fence progress = %+v, want timeout outcome",
					progress,
				)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("watermarker fence acquisition was not bounded and retried")
		}
	}
	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("two bounded fence attempts took %s", elapsed)
	}

	var terminated bool
	if err := pool.QueryRow(
		ctx,
		`SELECT pg_terminate_backend($1)`,
		writerPID,
	).Scan(&terminated); err != nil {
		t.Fatal(err)
	}
	if !terminated {
		t.Fatalf("writer backend %d was not terminated", writerPID)
	}

	for {
		select {
		case progress := <-observer.steps:
			if progress.Advanced && progress.SafeSeq >= target {
				stop()
				if err := <-runErr; err != nil {
					t.Fatal(err)
				}
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatalf(
				"watermarker did not advance through %d after writer termination",
				target,
			)
		}
	}
}

func TestWatermarkNoopSkipsPublicationAndThrottlesHeartbeat(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 10 * time.Millisecond,
		LeaseTTL:        time.Second,
		Owner:           fmt.Sprintf("noop-%d", time.Now().UnixNano()),
		InstallationID:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if progress, err := watermarker.Step(ctx); err != nil {
		t.Fatal(err)
	} else if progress.Advanced {
		t.Fatalf("idle step unexpectedly advanced: %+v", progress)
	}

	var updatedAt, firstLease time.Time
	var firstHeartbeats int64
	if err := pool.QueryRow(ctx, `
		SELECT watermark.updated_at, watermark.lease_until,
		       heartbeat.success_count
		FROM stream_watermark AS watermark
		JOIN operation_heartbeats AS heartbeat
		  ON heartbeat.installation_id = 1
		 AND heartbeat.component = 'watermarker'
		 AND heartbeat.operation = 'entities'
		WHERE watermark.singleton
	`).Scan(&updatedAt, &firstLease, &firstHeartbeats); err != nil {
		t.Fatal(err)
	}

	for range 5 {
		time.Sleep(10 * time.Millisecond)
		if progress, err := watermarker.Step(ctx); err != nil {
			t.Fatal(err)
		} else if progress.Advanced {
			t.Fatalf("idle step unexpectedly advanced: %+v", progress)
		}
	}
	var finalUpdatedAt, finalLease time.Time
	var finalHeartbeats int64
	if err := pool.QueryRow(ctx, `
		SELECT watermark.updated_at, watermark.lease_until,
		       heartbeat.success_count
		FROM stream_watermark AS watermark
		JOIN operation_heartbeats AS heartbeat
		  ON heartbeat.installation_id = 1
		 AND heartbeat.component = 'watermarker'
		 AND heartbeat.operation = 'entities'
		WHERE watermark.singleton
	`).Scan(
		&finalUpdatedAt,
		&finalLease,
		&finalHeartbeats,
	); err != nil {
		t.Fatal(err)
	}
	if !finalUpdatedAt.Equal(updatedAt) {
		t.Fatalf(
			"idle watermark publication changed updated_at from %s to %s",
			updatedAt,
			finalUpdatedAt,
		)
	}
	if !finalLease.After(firstLease) {
		t.Fatalf("idle lease was not renewed: %s -> %s", firstLease, finalLease)
	}
	if finalHeartbeats != firstHeartbeats {
		t.Fatalf(
			"heartbeat count changed inside one second: %d -> %d",
			firstHeartbeats,
			finalHeartbeats,
		)
	}

	time.Sleep(watermarkHeartbeatInterval)
	if _, err := watermarker.Step(ctx); err != nil {
		t.Fatal(err)
	}
	var renewedHeartbeats int64
	if err := pool.QueryRow(ctx, `
		SELECT success_count
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'watermarker'
		  AND operation = 'entities'
	`).Scan(&renewedHeartbeats); err != nil {
		t.Fatal(err)
	}
	if renewedHeartbeats != firstHeartbeats+1 {
		t.Fatalf(
			"heartbeat count after one second = %d, want %d",
			renewedHeartbeats,
			firstHeartbeats+1,
		)
	}
}

func TestChangeEventInsertRequiresWriterFence(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pool.Exec(ctx, `
		INSERT INTO change_events (stream, kind, entity_key, payload)
		VALUES ('fence-guard', 'test.changed', 'bare', '{"version":1}')
	`)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) ||
		postgresError.Code != "55000" ||
		!strings.Contains(postgresError.Message, "shared writer fence") {
		t.Fatalf("bare change-event insert error = %v", err)
	}

	if _, err := insertStreamEventWithKey(
		ctx,
		pool,
		"fence-guard",
		"fenced",
	); err != nil {
		t.Fatalf("fenced change-event insert: %v", err)
	}
}

func TestStorePoolBoundsIdleInTransactionSessions(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var bounded bool
	if err := pool.QueryRow(ctx, `
		SELECT current_setting('idle_in_transaction_session_timeout')::interval
		           > interval '0 seconds'
		   AND current_setting('idle_in_transaction_session_timeout')::interval
		           <= interval '5 minutes'
	`).Scan(&bounded); err != nil {
		t.Fatal(err)
	}
	if !bounded {
		t.Fatal("pool idle-in-transaction timeout is disabled or unbounded")
	}
}

func TestWatermarkerFencesRealEntityWriterAndDeriverTransactions(
	t *testing.T,
) {
	t.Parallel()
	for _, origin := range []string{
		outbox.EntityWriterOrigin,
		outbox.DeriverOrigin,
	} {
		for _, commit := range []bool{true, false} {
			name := origin + "/rollback"
			if commit {
				name = origin + "/commit"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testRealWriterFence(t, origin, commit)
			})
		}
	}
}

func TestEntityWriterUsesPostgresValidationAndCommitClock(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var before time.Time
	if err := pool.QueryRow(
		ctx,
		`SELECT clock_timestamp()`,
	).Scan(&before); err != nil {
		t.Fatal(err)
	}
	eventCtx := pipeline.WithEvent(ctx, before.Add(-time.Second))
	repositoryID := time.Now().UnixNano()
	_, err := store.NewEntityWriter(pool).ApplyRepository(
		eventCtx,
		store.RepositoryRecord{
			InstallationID:  1,
			OrgID:           1,
			GitHubID:        repositoryID,
			NodeID:          fmt.Sprintf("R_%d", repositoryID),
			Owner:           "clock",
			Name:            "repo",
			FullName:        "clock/repo",
			DefaultBranch:   "main",
			DefaultHeadSHA:  "head",
			GitHubUpdatedAt: before,
		},
		store.SyncSourceManual,
		"",
		before.Add(24*time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	var checkedAt, after time.Time
	if err := pool.QueryRow(ctx, `
		SELECT last_checked_at, clock_timestamp()
		FROM repos
		WHERE gh_id = $1
	`, repositoryID).Scan(&checkedAt, &after); err != nil {
		t.Fatal(err)
	}
	if checkedAt.Before(before) || checkedAt.After(after) {
		t.Fatalf(
			"validation timestamp %s is outside PostgreSQL interval [%s, %s]",
			checkedAt,
			before,
			after,
		)
	}
	committedAt := pipeline.CacheCommittedAt(eventCtx)
	if committedAt.Before(before) || committedAt.After(after) {
		t.Fatalf(
			"cache commit timestamp %s is outside PostgreSQL interval [%s, %s]",
			committedAt,
			before,
			after,
		)
	}
}

func testRealWriterFence(t *testing.T, origin string, commit bool) {
	t.Helper()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	allocated := make(chan int64, 1)
	release := make(chan struct{})
	rollbackErr := errors.New("force real writer rollback")
	writerCtx := outbox.WithSequenceAllocationHook(
		ctx,
		func(gotOrigin string, seq int64) error {
			if gotOrigin != origin {
				return nil
			}
			allocated <- seq
			<-release
			if !commit {
				return rollbackErr
			}
			return nil
		},
	)
	writerDone := make(chan error, 1)
	switch origin {
	case outbox.EntityWriterOrigin:
		go func() {
			now := time.Now().UTC()
			_, err := store.NewEntityWriter(pool).ApplyRepository(
				writerCtx,
				store.RepositoryRecord{
					InstallationID:  1,
					OrgID:           1,
					GitHubID:        time.Now().UnixNano(),
					NodeID:          fmt.Sprintf("R_%d", time.Now().UnixNano()),
					Owner:           "fence",
					Name:            "entity",
					FullName:        "fence/entity",
					DefaultBranch:   "main",
					DefaultHeadSHA:  "head",
					GitHubUpdatedAt: now,
				},
				store.SyncSourceManual,
				"",
				now,
			)
			writerDone <- err
		}()
	case outbox.DeriverOrigin:
		repositoryID := time.Now().UnixNano()
		scope := fmt.Sprintf("pr:1:%d:42", repositoryID)
		if _, err := pool.Exec(ctx, `
			INSERT INTO repos (
			    installation_id, org_id, gh_id, node_id, owner, name,
			    full_name, default_branch, archived, gh_updated_at,
			    head_sha, synced_at, last_checked_at, etag, sync_source
			)
			VALUES (
			    1, 1, $1, $2, 'fence', $3, $4, 'main', false,
			    clock_timestamp(), 'head', clock_timestamp(),
			    clock_timestamp(), '', 'manual'
			)
		`,
			repositoryID,
			fmt.Sprintf("R_%d", repositoryID),
			fmt.Sprintf("derive-%d", repositoryID),
			fmt.Sprintf("fence/derive-%d", repositoryID),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO derivation_dirty (scope_key, marked_at)
			VALUES ($1, clock_timestamp())
		`, scope); err != nil {
			t.Fatal(err)
		}
		service, err := derive.New(&derive.Options{
			Pool:           pool,
			InstallationID: 1,
			Deriver: fenceDeriver{
				identity: derive.PullRequestIdentity(repositoryID, 42),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			_, err := service.RunOnce(writerCtx)
			writerDone <- err
		}()
	default:
		t.Fatalf("unknown writer origin %q", origin)
	}
	seq := <-allocated

	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 10 * time.Millisecond,
		LeaseTTL:        time.Second,
		Owner: fmt.Sprintf(
			"real-%s-%t-%d",
			origin,
			commit,
			time.Now().UnixNano(),
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	beforeFence := make(chan struct{})
	watermarker.testBeforeFence = func() { close(beforeFence) }
	stepDone := make(chan error, 1)
	progress := make(chan WatermarkProgress, 1)
	go func() {
		got, err := watermarker.Step(ctx)
		progress <- got
		stepDone <- err
	}()
	<-beforeFence
	select {
	case err := <-stepDone:
		t.Fatalf(
			"watermarker crossed real %s transaction before commit/rollback: %v",
			origin,
			err,
		)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	writerErr := <-writerDone
	if commit && writerErr != nil {
		t.Fatalf("real %s commit: %v", origin, writerErr)
	}
	if !commit && !errors.Is(writerErr, rollbackErr) {
		t.Fatalf("real %s rollback error = %v", origin, writerErr)
	}
	if err := <-stepDone; err != nil {
		t.Fatal(err)
	}
	got := <-progress
	if commit && got.SafeSeq < seq {
		t.Fatalf(
			"real %s committed seq %d exceeds safe seq %d",
			origin,
			seq,
			got.SafeSeq,
		)
	}
	if !commit && got.SafeSeq >= seq {
		t.Fatalf(
			"real %s rolled-back seq %d was published at safe seq %d",
			origin,
			seq,
			got.SafeSeq,
		)
	}
}

type fenceDeriver struct {
	identity string
}

func (d fenceDeriver) Derive(
	snapshot derive.Snapshot,
) []derive.ScopeResult {
	results := make([]derive.ScopeResult, 0, len(snapshot.Scopes))
	for _, scope := range snapshot.Scopes {
		results = append(results, derive.ScopeResult{
			ScopeKey: scope.ScopeKey,
			WorkItems: []derive.WorkItem{{
				IdentityKey: d.identity,
				OrgID:       scope.OrgID,
				Payload:     json.RawMessage(`{"state":"fence"}`),
			}},
		})
	}
	return results
}

func TestRetentionNeverPrunesAboveSafeWatermark(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	old := time.Now().Add(-8 * 24 * time.Hour)

	smaller, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer smaller.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, smaller); err != nil {
		t.Fatal(err)
	}
	smallSeq, err := insertStreamEventTx(
		ctx, smaller, "retention-safe", "small", old,
	)
	if err != nil {
		t.Fatal(err)
	}
	largeSeq, err := insertStreamEventWithTime(
		ctx, pool, "retention-safe", "large", old,
	)
	if err != nil {
		t.Fatal(err)
	}
	if smallSeq >= largeSeq {
		t.Fatalf("sequences small=%d large=%d", smallSeq, largeSeq)
	}

	retention, err := NewRetention(pool, RetentionOptions{
		Age:       minimumRetentionAge,
		Period:    time.Hour,
		BatchSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := retention.Prune(ctx); err != nil || deleted != 0 {
		t.Fatalf("pruned above-safe committed event: deleted=%d err=%v", deleted, err)
	}
	var safe, horizon int64
	if err := pool.QueryRow(ctx, `
		SELECT watermark.safe_seq,
		       COALESCE(horizons.pruned_through_seq, 0)
		FROM stream_watermark AS watermark
		LEFT JOIN stream_horizons AS horizons
		  ON horizons.stream = 'retention-safe'
		WHERE watermark.singleton
	`).Scan(&safe, &horizon); err != nil {
		t.Fatal(err)
	}
	if horizon > safe {
		t.Fatalf("pruned horizon %d exceeds safe seq %d", horizon, safe)
	}
	var retained int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events WHERE seq = $1
	`, largeSeq).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatalf("above-safe event retained=%d, want 1", retained)
	}
}

func TestRetentionSevenDayFloor(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	if _, err := NewRetention(pool, RetentionOptions{
		Age:       minimumRetentionAge - time.Second,
		Period:    time.Hour,
		BatchSize: 10,
	}); err == nil {
		t.Fatal("retention accepted less than the C-S7 seven-day floor")
	}
	if _, err := NewRetention(pool, RetentionOptions{
		Age:       minimumRetentionAge,
		Period:    time.Hour,
		BatchSize: 10,
	}); err != nil {
		t.Fatalf("seven-day retention rejected: %v", err)
	}
}

func TestStatementLevelWakeTriggersEmitOneConstantNotification(t *testing.T) {
	t.Parallel()
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assertOneStatementWake(
		t,
		ctx,
		pool,
		"ghsync_change_events",
		"changed",
		func(tx pgx.Tx) error {
			if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `
				INSERT INTO change_events (
				    stream, kind, entity_key, payload
				)
				SELECT 'trigger-test-' || value::text,
				       'test.changed',
				       value::text,
				       '{"version":1}'
				FROM generate_series(1, 25) AS value
			`)
			return err
		},
	)
	assertOneStatementWake(
		t,
		ctx,
		pool,
		"ghsync_derivation_dirty",
		"dirty",
		func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `
				INSERT INTO derivation_dirty (scope_key, marked_at)
				SELECT 'pr:1:1:' || value::text, clock_timestamp()
				FROM generate_series(1, 25) AS value
			`)
			return err
		},
	)
}

func assertOneStatementWake(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	channel string,
	payload string,
	write func(pgx.Tx) error,
) {
	t.Helper()
	listener, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, "LISTEN "+channel); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	var writerPID uint32
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&writerPID); err != nil {
		t.Fatal(err)
	}
	if err := write(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var notification *pgconn.Notification
	for notification == nil || notification.PID != writerPID {
		notification, err = listener.Conn().WaitForNotification(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	if notification.Channel != channel || notification.Payload != payload {
		t.Fatalf(
			"notification = %s/%q, want %s/%q",
			notification.Channel,
			notification.Payload,
			channel,
			payload,
		)
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		extraCtx, cancel := context.WithDeadline(ctx, deadline)
		extra, err := listener.Conn().WaitForNotification(extraCtx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if err != nil {
			t.Fatalf("wait for extra notification: %v", err)
		}
		if extra.PID == writerPID {
			t.Fatalf("extra per-row notification: %+v", extra)
		}
	}
}

func waitForExclusiveFenceHolder(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) int32 {
	t.Helper()
	fenceKey := uint64(outbox.FenceKey)
	classID := int64(fenceKey >> 32)
	objectID := int64(uint32(fenceKey))
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		var pid int32
		err := pool.QueryRow(ctx, `
			SELECT pid
			FROM pg_locks
			WHERE locktype = 'advisory'
			  AND database = (
			      SELECT oid FROM pg_database
			      WHERE datname = current_database()
			  )
			  AND classid = $1
			  AND objid = $2
			  AND objsubid = 1
			  AND mode = 'ExclusiveLock'
			  AND granted
			LIMIT 1
		`, classID, objectID).Scan(&pid)
		if err == nil {
			return pid
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatal("timed out waiting for watermarker exclusive fence")
		case <-ticker.C:
		}
	}
}

func waitForFenceLocks(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	mode string,
	granted bool,
	want int,
) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		count := fenceLockCount(t, ctx, pool, mode, granted)
		if count >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatalf(
				"timed out waiting for %d outbox fence locks (%s, granted=%t)",
				want,
				mode,
				granted,
			)
		case <-ticker.C:
		}
	}
}

func fenceLockCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	mode string,
	granted bool,
) int {
	t.Helper()
	fenceKey := uint64(outbox.FenceKey)
	classID := int64(fenceKey >> 32)
	objectID := int64(uint32(fenceKey))
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND database = (
		      SELECT oid FROM pg_database
		      WHERE datname = current_database()
		  )
		  AND classid = $1
		  AND objid = $2
		  AND objsubid = 1
		  AND mode = $3
		  AND granted = $4
	`, classID, objectID, mode, granted).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func waitForBackendBlockedBy(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	blockerPID int32,
) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
			    SELECT 1
			    FROM pg_stat_activity
			    WHERE $1 = ANY(pg_blocking_pids(pid))
			)
		`, blockerPID).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"no backend blocked behind heartbeat holder %d: %v",
				blockerPID,
				ctx.Err(),
			)
		case <-deadline.C:
			t.Fatalf(
				"timed out waiting for backend blocked behind heartbeat holder %d",
				blockerPID,
			)
		case <-ticker.C:
		}
	}
}

func assertFenceObservation(
	t *testing.T,
	ctx context.Context,
	observations <-chan outbox.FenceObservation,
	wantRole outbox.FenceRole,
) {
	t.Helper()
	var observation outbox.FenceObservation
	select {
	case observation = <-observations:
	case <-ctx.Done():
		t.Fatalf("fence observation was not emitted: %v", ctx.Err())
	}
	if !observation.Acquired || observation.Role != wantRole {
		t.Fatalf(
			"fence observation = %+v, want acquired role %q",
			observation,
			wantRole,
		)
	}
	if observation.WaitDuration <= 0 || observation.HoldDuration <= 0 {
		t.Fatalf(
			"fence observation did not separate positive wait and hold: %+v",
			observation,
		)
	}
}

func insertStreamEvent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
) int64 {
	t.Helper()
	seq, err := insertStreamEventWithKey(ctx, pool, streamName, "idle")
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func insertStreamEventWithKey(
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
	key string,
) (int64, error) {
	return insertStreamEventWithTime(ctx, pool, streamName, key, time.Now())
}

func insertStreamEventWithTime(
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
	key string,
	occurredAt time.Time,
) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		return 0, err
	}
	seq, err := insertStreamEventTx(ctx, tx, streamName, key, occurredAt)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return seq, nil
}

func insertStreamEventTx(
	ctx context.Context,
	tx pgx.Tx,
	streamName string,
	key string,
	occurredAt time.Time,
) (int64, error) {
	var seq int64
	err := tx.QueryRow(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, occurred_at, payload
		)
		VALUES ($1, 'test.changed', $2, $3, '{"version":1}')
		RETURNING seq
	`, streamName, key, occurredAt).Scan(&seq)
	return seq, err
}

func waitSafeSequence(
	t *testing.T,
	pool *pgxpool.Pool,
	target int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var safe int64
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(context.Background(), `
			SELECT safe_seq FROM stream_watermark WHERE singleton
		`).Scan(&safe); err != nil {
			t.Fatal(err)
		}
		if safe >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("safe watermark = %d, want at least %d", safe, target)
}

type watermarkProgressRecorder struct {
	steps chan WatermarkProgress
}

type fenceObservationRecorder struct {
	observations chan outbox.FenceObservation
}

func (r *fenceObservationRecorder) OutboxFence(
	_ context.Context,
	observation outbox.FenceObservation,
) {
	r.observations <- observation
}

type convoyWatermarkObserver struct {
	fences chan outbox.FenceObservation
}

func (r *convoyWatermarkObserver) OutboxFence(
	_ context.Context,
	observation outbox.FenceObservation,
) {
	r.fences <- observation
}

func (r *convoyWatermarkObserver) WatermarkStep(
	_ context.Context,
	_ WatermarkProgress,
) {
}

func (r *watermarkProgressRecorder) WatermarkStep(
	_ context.Context,
	progress WatermarkProgress,
) {
	r.steps <- progress
}

func streamDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return testdb.New(t).Pool
}
