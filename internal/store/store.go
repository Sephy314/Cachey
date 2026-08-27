package store

type Store interface {
	Get(key string) (*string, error)
	Put(key string, value string) error
	Delete(key string) error
	Alive() bool
}

type CacheyStore struct {
	data map[string]string
}

func NewCacheyStore() *CacheyStore {
	return &CacheyStore{
		data: make(map[string]string),
	}
}

func (s *CacheyStore) Get(key string) (*string, error) {
	value, ok := s.data[key]
	if !ok {
		return nil, ErrorCodeInvalidKey
	}
	return &value, nil
}

func (s *CacheyStore) Put(key string, value string) error {
	s.data[key] = value
	return nil
}

func (s *CacheyStore) Delete(key string) error {
	_, ok := s.data[key]
	if !ok {
		return ErrorCodeInvalidKey
	}
	delete(s.data, key)
	return nil
}

func (s *CacheyStore) Alive() bool {
	return true
}