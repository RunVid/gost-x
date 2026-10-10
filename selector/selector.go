package selector

import (
	"context"
	"time"

	"github.com/go-gost/core/selector"
)

// default options for FailFilter
const (
	DefaultMaxFails    = 1
	DefaultFailTimeout = 10 * time.Second
)

const (
	labelWeight      = "weight"
	labelBackup      = "backup"
	labelMaxFails    = "maxFails"
	labelFailTimeout = "failTimeout"
)

type defaultSelector[T any] struct {
	strategy selector.Strategy[T]
	filters  []selector.Filter[T]
}

func NewSelector[T any](strategy selector.Strategy[T], filters ...selector.Filter[T]) selector.Selector[T] {
	return &defaultSelector[T]{
		filters:  filters,
		strategy: strategy,
	}
}

func (s *defaultSelector[T]) Select(ctx context.Context, vs ...T) (v T) {
	for _, filter := range s.filters {
		vs = filter.Filter(ctx, vs...)
	}
	if len(vs) == 0 {
		return
	}
	return s.strategy.Apply(ctx, vs...)
}

// FailFiltered returns the members of vs that the FailFilter of s drops now,
// those cooling down after failures. Filters before it are applied first, as
// Select does. It returns nil when s was not made by NewSelector or has no
// FailFilter.
func FailFiltered[T comparable](ctx context.Context, s selector.Selector[T], vs ...T) []T {
	ds, ok := s.(*defaultSelector[T])
	if !ok {
		return nil
	}
	for _, filter := range ds.filters {
		if _, ok := filter.(*failFilter[T]); !ok {
			vs = filter.Filter(ctx, vs...)
			continue
		}
		kept := make(map[T]struct{}, len(vs))
		for _, v := range filter.Filter(ctx, vs...) {
			kept[v] = struct{}{}
		}
		var dropped []T
		for _, v := range vs {
			if _, ok := kept[v]; !ok {
				dropped = append(dropped, v)
			}
		}
		return dropped
	}
	return nil
}
