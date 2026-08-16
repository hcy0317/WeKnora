package repository

import (
	"context"

	"gorm.io/gorm"
)

type guardedDBContextKey struct{}

func withGuardedDB(ctx context.Context, db *gorm.DB) context.Context {
	return context.WithValue(ctx, guardedDBContextKey{}, db)
}

func dbWithContext(ctx context.Context, fallback *gorm.DB) *gorm.DB {
	if guarded, ok := guardedDBFromContext(ctx); ok {
		return guarded.WithContext(ctx)
	}
	return fallback.WithContext(ctx)
}

func guardedDBFromContext(ctx context.Context) (*gorm.DB, bool) {
	guarded, ok := ctx.Value(guardedDBContextKey{}).(*gorm.DB)
	return guarded, ok && guarded != nil
}
