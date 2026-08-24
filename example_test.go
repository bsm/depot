package depot_test

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/bsm/depot"
)

// User is the record type exchanged in the examples. With an .ndjson URL every
// record becomes one JSON line.
type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func ExampleProduce() {
	ctx := context.Background()

	users := []*User{
		{ID: 1, Name: "Jane"},
		{ID: 2, Name: "Joe"},
	}
	records := func(emit func(*User) error) error {
		for _, u := range users {
			if err := emit(u); err != nil {
				return err
			}
		}
		return nil
	}

	// version is any monotonic stamp that changes when the data changes,
	// e.g. max(updated_at) as unix nanos.
	const version = 101

	status, err := depot.Produce(ctx, "mem://example-produce/users.ndjson", version, records)
	if err != nil {
		panic(err)
	}
	fmt.Printf("produced items:%d skipped:%v\n", status.NumItems, status.Skipped)

	// running again with the same version skips the write entirely
	status, err = depot.Produce(ctx, "mem://example-produce/users.ndjson", version, records)
	if err != nil {
		panic(err)
	}
	fmt.Printf("produced items:%d skipped:%v\n", status.NumItems, status.Skipped)

	// Output:
	// produced items:2 skipped:false
	// produced items:0 skipped:true
}

func ExampleSubscribe() {
	ctx := context.Background()
	const url = "mem://example-subscribe/users.ndjson"

	// seed a snapshot for the example
	if _, err := depot.Produce(ctx, url, 101, func(emit func(*User) error) error {
		if err := emit(&User{ID: 1, Name: "Jane"}); err != nil {
			return err
		}
		return emit(&User{ID: 2, Name: "Joe"})
	}); err != nil {
		panic(err)
	}

	// Subscribe loads the snapshot now and refreshes it every minute in the
	// background. build turns the item stream into the value returned by Load;
	// here it is a lookup map by ID.
	sub, err := depot.Subscribe(ctx, url, time.Minute,
		func(rows iter.Seq[*User]) (map[int]*User, error) {
			byID := make(map[int]*User)
			for u := range rows {
				byID[u.ID] = u
			}
			return byID, nil
		})
	if err != nil {
		panic(err)
	}
	defer func() { _ = sub.Close() }()

	// hot path: lock-free read of the latest snapshot
	fmt.Printf("version:%d user:%s\n", sub.Version(), sub.Load()[2].Name)

	// Output:
	// version:101 user:Joe
}
