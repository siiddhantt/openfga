package graph

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openfga/openfga/pkg/storage"
)

type cachedTuple struct {
	user      string
	condition *openfgav1.RelationshipCondition
	timestamp *timestamppb.Timestamp
}

type cachedTupleIterator struct {
	object   string
	relation string
	iter     storage.Iterator[cachedTuple]
}

var _ storage.TupleIterator = (*cachedTupleIterator)(nil)

// Next see [Iterator.Next].
func (c *cachedTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	cachedTuple := &openfgav1.Tuple{
		Key: &openfgav1.TupleKey{
			User:      t.user,
			Object:    c.object,
			Relation:  c.relation,
			Condition: t.condition,
		},
		Timestamp: t.timestamp,
	}

	return cachedTuple, nil
}

// Stop see [Iterator.Stop].
func (c *cachedTupleIterator) Stop() {
	c.iter.Stop()
}

// Head see [Iterator.Head].
func (c *cachedTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Head(ctx)
	if err != nil {
		return nil, err
	}

	cachedTuple := &openfgav1.Tuple{
		Key: &openfgav1.TupleKey{
			User:      t.user,
			Object:    c.object,
			Relation:  c.relation,
			Condition: t.condition,
		},
		Timestamp: t.timestamp,
	}

	return cachedTuple, nil
}
