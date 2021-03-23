package view

type applyContextImpl struct {
	db DB
}

func newApplyContext(db DB) *applyContextImpl {
	return &applyContextImpl{db}
}

func (c *applyContextImpl) Get(key []byte) ([]byte, error) {
	return c.db.Get(key)
}

func (c *applyContextImpl) Set(key, value []byte) error {
	return c.db.Set(key, value)
}

func (c *applyContextImpl) Delete(key []byte) error {
	return c.db.Delete(key)
}
