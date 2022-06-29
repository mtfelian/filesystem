package filesystem

import (
	"context"
	"sync"
)

type (
	// cbCtxFunc is callback func type with context propagation
	cbCtxFunc func(context.Context) (context.Context, error)

	// cbFunc is callback func type
	cbFunc func(context.Context) error
)

// callbacks
var (
	cbMu              sync.Mutex
	beforeOperationCB cbCtxFunc
	afterOperationCB  cbFunc
)

// BeforeOperationCB returns callback that will be invoked before each operation
func BeforeOperationCB() cbCtxFunc {
	cbMu.Lock()
	defer cbMu.Unlock()
	return beforeOperationCB
}

// SetBeforeOperationCB sets callback that will be invoked before each operation
func SetBeforeOperationCB(f cbCtxFunc) {
	cbMu.Lock()
	defer cbMu.Unlock()
	beforeOperationCB = f
}

// AfterOperationCB returns callback that will be invoked after each operation
func AfterOperationCB() cbFunc {
	cbMu.Lock()
	defer cbMu.Unlock()
	return afterOperationCB
}

// SetAfterOperationCB sets callback that will be invoked after each operation
func SetAfterOperationCB(f cbFunc) {
	cbMu.Lock()
	defer cbMu.Unlock()
	afterOperationCB = f
}

func invokeBeforeOperationCB(ctx context.Context) (context.Context, error) {
	cb := BeforeOperationCB()
	if cb == nil {
		return ctx, nil
	}
	return cb(ctx)
}

func invokeAfterOperationCB(ctx context.Context) error {
	cb := AfterOperationCB()
	if cb == nil {
		return nil
	}
	return cb(ctx)
}
