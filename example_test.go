package depot_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/bsm/depot"
	"github.com/bsm/depot/internal/testdata"
)

type message = testdata.MockMessage

func ExampleProduce() {
	ctx := context.TODO()

	status, err := depot.Produce(ctx, "mem://example-produce/todos.ndjson", 101,
		func(emit func(*message) error) error {
			return errors.Join(
				emit(&message{Name: "Jane", Height: 175}),
				emit(&message{Name: "Joe", Height: 172}),
			)
		})
	if err != nil {
		panic(err)
	}

	fmt.Printf("PRODUCED skipped:%v version:%v->%v items:%v\n", status.Skipped, status.LocalVersion, status.RemoteVersion, status.NumItems)

	// Output:
	// PRODUCED skipped:false version:101->0 items:2
}

func ExampleSubscribe() {
	ctx := context.TODO()

	const url = "mem://example-subscribe/todos.ndjson"
	if _, err := depot.Produce(ctx, url, 101, func(emit func(*message) error) error {
		return errors.Join(
			emit(&message{Name: "Jane", Height: 175}),
			emit(&message{Name: "Joe", Height: 172}),
		)
	}); err != nil {
		panic(err)
	}

	// Subscribe loads the snapshot once and refreshes it every minute in the
	// background. build turns the item stream into the value returned by Load.
	sub, err := depot.Subscribe(ctx, url, time.Minute,
		func(rows iter.Seq[*message]) ([]string, error) {
			var names []string
			for msg := range rows {
				names = append(names, msg.Name)
			}
			return names, nil
		})
	if err != nil {
		panic(err)
	}
	defer func() { _ = sub.Close() }()

	fmt.Printf("COLLECTED version:%v names:%q\n", sub.Version(), sub.Load())

	// Output:
	// COLLECTED version:101 names:["Jane" "Joe"]
}
