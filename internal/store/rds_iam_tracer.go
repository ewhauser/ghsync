package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type connectionConfigContextKey struct{}

type connectionConfigHolder struct {
	config *pgx.ConnConfig
}

func (*connectionConfigHolder) String() string {
	return "RDS IAM connection config (redacted)"
}

// passwordScrubbingTracer owns the one point at which pgx's connection-local
// config can be cleared before pgx returns it in a connection or ConnectError.
// It also prevents an instrumentation tracer from observing the token.
type passwordScrubbingTracer struct {
	delegate pgx.QueryTracer
}

func newPasswordScrubbingTracer(delegate pgx.QueryTracer) *passwordScrubbingTracer {
	return &passwordScrubbingTracer{delegate: delegate}
}

func (t *passwordScrubbingTracer) TraceConnectStart(
	ctx context.Context,
	data pgx.TraceConnectStartData,
) (result context.Context) {
	actual := data.ConnConfig
	token := ""
	if actual != nil {
		token = actual.Password
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if actual != nil {
				actual.Password = ""
			}
			panic(redactedPanicValue(recovered, token))
		}
	}()

	if delegate, ok := t.delegate.(pgx.ConnectTracer); ok {
		safeData := data
		if actual != nil {
			safeData.ConnConfig = actual.Copy()
			safeData.ConnConfig.Password = ""
		}
		ctx = delegate.TraceConnectStart(ctx, safeData)
	}
	return context.WithValue(
		ctx,
		connectionConfigContextKey{},
		&connectionConfigHolder{config: actual},
	)
}

func (t *passwordScrubbingTracer) TraceConnectEnd(
	ctx context.Context,
	data pgx.TraceConnectEndData,
) {
	if holder, ok := ctx.Value(connectionConfigContextKey{}).(*connectionConfigHolder); ok &&
		holder.config != nil {
		holder.config.Password = ""
	}
	var connectError *pgconn.ConnectError
	if errors.As(data.Err, &connectError) && connectError.Config != nil {
		connectError.Config.Password = ""
	}
	if delegate, ok := t.delegate.(pgx.ConnectTracer); ok {
		delegate.TraceConnectEnd(ctx, data)
	}
}

func redactedPanicValue(value any, token string) string {
	message := fmt.Sprint(value)
	if token != "" {
		message = strings.ReplaceAll(message, token, "[REDACTED]")
	}
	return message
}

func (t *passwordScrubbingTracer) TraceQueryStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	if t.delegate == nil {
		return ctx
	}
	return t.delegate.TraceQueryStart(ctx, conn, data)
}

func (t *passwordScrubbingTracer) TraceQueryEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryEndData,
) {
	if t.delegate != nil {
		t.delegate.TraceQueryEnd(ctx, conn, data)
	}
}

func (t *passwordScrubbingTracer) TraceBatchStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceBatchStartData,
) context.Context {
	if delegate, ok := t.delegate.(pgx.BatchTracer); ok {
		return delegate.TraceBatchStart(ctx, conn, data)
	}
	return ctx
}

func (t *passwordScrubbingTracer) TraceBatchQuery(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceBatchQueryData,
) {
	if delegate, ok := t.delegate.(pgx.BatchTracer); ok {
		delegate.TraceBatchQuery(ctx, conn, data)
	}
}

func (t *passwordScrubbingTracer) TraceBatchEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceBatchEndData,
) {
	if delegate, ok := t.delegate.(pgx.BatchTracer); ok {
		delegate.TraceBatchEnd(ctx, conn, data)
	}
}

func (t *passwordScrubbingTracer) TraceCopyFromStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceCopyFromStartData,
) context.Context {
	if delegate, ok := t.delegate.(pgx.CopyFromTracer); ok {
		return delegate.TraceCopyFromStart(ctx, conn, data)
	}
	return ctx
}

func (t *passwordScrubbingTracer) TraceCopyFromEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceCopyFromEndData,
) {
	if delegate, ok := t.delegate.(pgx.CopyFromTracer); ok {
		delegate.TraceCopyFromEnd(ctx, conn, data)
	}
}

func (t *passwordScrubbingTracer) TracePrepareStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TracePrepareStartData,
) context.Context {
	if delegate, ok := t.delegate.(pgx.PrepareTracer); ok {
		return delegate.TracePrepareStart(ctx, conn, data)
	}
	return ctx
}

func (t *passwordScrubbingTracer) TracePrepareEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TracePrepareEndData,
) {
	if delegate, ok := t.delegate.(pgx.PrepareTracer); ok {
		delegate.TracePrepareEnd(ctx, conn, data)
	}
}

func (t *passwordScrubbingTracer) TraceAcquireStart(
	ctx context.Context,
	pool *pgxpool.Pool,
	data pgxpool.TraceAcquireStartData,
) context.Context {
	if delegate, ok := t.delegate.(pgxpool.AcquireTracer); ok {
		return delegate.TraceAcquireStart(ctx, pool, data)
	}
	return ctx
}

func (t *passwordScrubbingTracer) TraceAcquireEnd(
	ctx context.Context,
	pool *pgxpool.Pool,
	data pgxpool.TraceAcquireEndData,
) {
	if delegate, ok := t.delegate.(pgxpool.AcquireTracer); ok {
		delegate.TraceAcquireEnd(ctx, pool, data)
	}
}

func (t *passwordScrubbingTracer) TraceRelease(
	pool *pgxpool.Pool,
	data pgxpool.TraceReleaseData,
) {
	if delegate, ok := t.delegate.(pgxpool.ReleaseTracer); ok {
		delegate.TraceRelease(pool, data)
	}
}
