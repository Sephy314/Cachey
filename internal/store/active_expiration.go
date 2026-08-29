package store

import "time"

// maxCleanupPerRun caps how many expired keys a single active-expiration pass
// removes, so cleanup never blocks the store for an unbounded amount of time.
const maxCleanupPerRun = 100

// cleanupExpired removes up to limit expired keys, starting from the
// soonest-expiring entry in the index. It never scans the full data map.
func (s *CacheyStore) cleanupExpired(limit int) int {
	now := time.Now().UnixMilli()
	removed := 0
	for removed < limit {
		e, found := s.index.First()
		if !found || e.Exp > now {
			break
		}

		s.mu.Lock()
		entry, ok := s.data[e.Key]
		stillExpired := ok && entry.Exp == e.Exp && s.isExpired(entry)
		if stillExpired {
			delete(s.data, e.Key)
		}
		s.mu.Unlock()

		// Drop the index entry regardless: either it was just expired, or it
		// was stale (superseded by a later PUT/TTL) and must not block progress.
		s.index.Delete(e.Exp, e.Key)
		if stillExpired {
			removed++
		}
	}
	return removed
}

// StartActiveExpiration launches a background worker that periodically
// purges expired keys via cleanupExpired. Call the returned function to stop it.
func (s *CacheyStore) StartActiveExpiration(interval time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.cleanupExpired(maxCleanupPerRun)
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}
