package psp

import "fmt"

// Registry resolves a provider by name.
//
// Routing rules — choosing a provider by currency, cost, or health — are a
// later phase. This exists now so that the service depends on a lookup rather
// than on a concrete adapter, which is what will let routing slot in without
// touching the orchestration logic.
type Registry struct {
	adapters    map[string]Adapter
	defaultName string
}

func NewRegistry(defaultName string, adapters ...Adapter) *Registry {
	byName := make(map[string]Adapter, len(adapters))
	for _, a := range adapters {
		byName[a.Name()] = a
	}
	return &Registry{adapters: byName, defaultName: defaultName}
}

func (r *Registry) Get(name string) (Adapter, error) {
	a, ok := r.adapters[name]
	if !ok {
		return nil, fmt.Errorf("no payment provider registered as %q", name)
	}
	return a, nil
}

// Default returns the provider used when a transaction does not name one.
func (r *Registry) Default() (Adapter, error) { return r.Get(r.defaultName) }

// Names lists every registered provider, for diagnostics and the health probe.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		out = append(out, name)
	}
	return out
}
