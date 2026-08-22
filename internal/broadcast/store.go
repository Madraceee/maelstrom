package broadcast

import "sync"

type store struct {
	mu    sync.RWMutex
	cache map[int]bool
	store []int
}

func NewStore() *store {
	return &store{
		mu:    sync.RWMutex{},
		store: make([]int, 0),
		cache: make(map[int]bool),
	}
}

func (s *store) Store(value int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if isPresent := s.cache[value]; isPresent {
		return
	}
	s.store = append(s.store, value)
	s.cache[value] = true
}

func (s *store) StoreMultiple(values []int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	newValues := make([]int, 0)
	for _, value := range values {
		if isPresent := s.cache[value]; isPresent {
			continue
		}
		s.store = append(s.store, value)
		s.cache[value] = true
		newValues = append(newValues, value)
	}
	return newValues
}

func (s *store) Get() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	storeCopy := make([]int, len(s.store))
	copy(storeCopy, s.store)
	return storeCopy
}
