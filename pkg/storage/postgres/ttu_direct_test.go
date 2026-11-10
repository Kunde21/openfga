package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

func TestReadTTUDirectMatches(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	ds, err := New(testDatastore.GetConnectionURI(true), sqlcommon.NewConfig(sqlcommon.WithMaxTuplesPerWrite(1000)))
	require.NoError(t, err)
	t.Cleanup(ds.Close)

	ctx := context.Background()
	store := ulid.Make().String()
	cond, err := structpb.NewStruct(map[string]any{"x": "1"})
	require.NoError(t, err)
	require.NoError(t, ds.Write(ctx, store, nil, []*openfgav1.TupleKey{
		tupleUtils.NewTupleKey("doc:1", "parent", "folder:a"),
		tupleUtils.NewTupleKeyWithCondition("doc:1", "parent", "folder:b", "cond", cond),
		tupleUtils.NewTupleKey("doc:1", "parent", "folder:d"),
		tupleUtils.NewTupleKey("doc:1", "parent", "folder:e"),
		tupleUtils.NewTupleKey("doc:1", "parent", "group:a"),  // other parent type
		tupleUtils.NewTupleKey("doc:1", "owner", "folder:c"),  // other tupleset relation
		tupleUtils.NewTupleKey("doc:2", "parent", "folder:c"), // other object
		tupleUtils.NewTupleKey("folder:a", "viewer", "user:anne"),
		tupleUtils.NewTupleKey("folder:a", "editor", "user:anne"), // other computed relation
		tupleUtils.NewTupleKey("folder:b", "viewer", "user:*"),
		tupleUtils.NewTupleKey("folder:c", "viewer", "user:anne"),
		tupleUtils.NewTupleKey("folder:d", "viewer", "user:anne"),
		tupleUtils.NewTupleKeyWithCondition("folder:e", "viewer", "user:anne", "cond", cond),
		tupleUtils.NewTupleKey("group:a", "viewer", "user:anne"),
		tupleUtils.NewTupleKey("folder:a", "viewer", "user:bob"),
		tupleUtils.NewTupleKey("doc:3", "parent", "folder:a"),
		tupleUtils.NewTupleKey("doc:4", "parent", "folder:f"),
		tupleUtils.NewTupleKey("folder:f", "editor", "user:anne"), // other computed relation only
	}))

	// read returns the matches as strings, with the condition name and its x parameter if any.
	read := func(t *testing.T, filter storage.TTUDirectFilter) []string {
		str := func(tk *openfgav1.TupleKey) string {
			s := tupleUtils.TupleKeyToString(tk)
			if c := tk.GetCondition(); c != nil {
				s += " with " + c.GetName() + " x=" + c.GetContext().GetFields()["x"].GetStringValue()
			}
			return s
		}
		var got []string
		ok, err := ds.ReadTTUDirectMatches(ctx, store, filter, storage.ReadOptions{}, func(m storage.TTUDirectMatch) bool {
			got = append(got, str(m.Tupleset)+" -> "+str(m.Parent))
			return true
		})
		require.NoError(t, err)
		require.True(t, ok)
		return got
	}
	filter := storage.TTUDirectFilter{
		Object:           "doc:1",
		TuplesetRelation: "parent",
		ParentTypes:      []string{"folder"},
		ComputedRelation: "viewer",
		Users:            []string{"user:anne", "user:*"},
		Unconditioned:    []storage.TTUDirectGrant{{ParentType: "folder", User: "user:anne"}},
		Conditioned:      true,
	}
	conditioned := []string{
		"doc:1#parent@folder:b with cond x=1 -> folder:b#viewer@user:*",
		"doc:1#parent@folder:e -> folder:e#viewer@user:anne with cond x=1",
	}

	t.Run("one_listed_unconditioned_match_alone", func(t *testing.T) {
		got := read(t, filter)
		require.Len(t, got, 1)
		// folder:a and folder:d both match without conditions; one of them is returned.
		require.Subset(t, []string{
			"doc:1#parent@folder:a -> folder:a#viewer@user:anne",
			"doc:1#parent@folder:d -> folder:d#viewer@user:anne",
		}, got)
	})

	t.Run("every_conditioned_match_when_no_unconditioned_match_is_listed", func(t *testing.T) {
		f := filter
		f.Unconditioned = []storage.TTUDirectGrant{{ParentType: "folder", User: "user:*"}, {ParentType: "group", User: "user:anne"}}
		require.ElementsMatch(t, conditioned, read(t, f))

		// doc:3 has one match and it is not listed.
		f.Object = "doc:3"
		require.Empty(t, read(t, f))
		f.Unconditioned = filter.Unconditioned
		require.Equal(t, []string{"doc:3#parent@folder:a -> folder:a#viewer@user:anne"}, read(t, f))
	})

	t.Run("no_conditioned_match_when_the_model_accepts_none", func(t *testing.T) {
		f := filter
		f.Unconditioned = nil
		f.Conditioned = false
		require.Empty(t, read(t, f))
	})

	t.Run("conditioned_matches_over_several_pages", func(t *testing.T) {
		// doc:page has 2*ttuDirectPageSize+41 matches, all with a condition. The first parent has
		// one match and the others have two, so a page ends between the two matches of a parent.
		const parents = ttuDirectPageSize + 21
		tuples := []*openfgav1.TupleKey{}
		var want []string
		for i := range parents {
			folder := fmt.Sprintf("folder:p%03d", i)
			tuples = append(tuples,
				tupleUtils.NewTupleKey("doc:page", "parent", folder),
				tupleUtils.NewTupleKeyWithCondition(folder, "viewer", "user:anne", "cond", cond))
			want = append(want, "doc:page#parent@"+folder+" -> "+folder+"#viewer@user:anne with cond x=1")
			if i > 0 {
				tuples = append(tuples, tupleUtils.NewTupleKeyWithCondition(folder, "viewer", "user:*", "cond", cond))
				want = append(want, "doc:page#parent@"+folder+" -> "+folder+"#viewer@user:* with cond x=1")
			}
		}
		require.NoError(t, ds.Write(ctx, store, nil, tuples))

		f := filter
		f.Object = "doc:page"
		require.ElementsMatch(t, want, read(t, f))

		calls := 0
		ok, err := ds.ReadTTUDirectMatches(ctx, store, f, storage.ReadOptions{}, func(storage.TTUDirectMatch) bool {
			calls++
			return calls < ttuDirectPageSize+50
		})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, ttuDirectPageSize+50, calls)
	})

	t.Run("unconditioned_match_on_a_later_page", func(t *testing.T) {
		// The parents of doc:late fill one page with conditioned matches. The next page has one
		// match without conditions and one with.
		tuples := []*openfgav1.TupleKey{
			tupleUtils.NewTupleKey("doc:late", "parent", "folder:r"),
			tupleUtils.NewTupleKey("folder:r", "viewer", "user:anne"),
			tupleUtils.NewTupleKey("doc:late", "parent", "folder:s"),
			tupleUtils.NewTupleKeyWithCondition("folder:s", "viewer", "user:anne", "cond", cond),
		}
		want := []string{"doc:late#parent@folder:r -> folder:r#viewer@user:anne"}
		for i := range ttuDirectPageSize {
			folder := fmt.Sprintf("folder:q%03d", i)
			tuples = append(tuples,
				tupleUtils.NewTupleKey("doc:late", "parent", folder),
				tupleUtils.NewTupleKeyWithCondition(folder, "viewer", "user:anne", "cond", cond))
			want = append(want, "doc:late#parent@"+folder+" -> "+folder+"#viewer@user:anne with cond x=1")
		}
		require.NoError(t, ds.Write(ctx, store, nil, tuples))

		f := filter
		f.Object = "doc:late"
		require.ElementsMatch(t, want, read(t, f))
	})

	t.Run("stops_when_yield_returns_false", func(t *testing.T) {
		calls := 0
		ok, err := ds.ReadTTUDirectMatches(ctx, store, filter, storage.ReadOptions{}, func(storage.TTUDirectMatch) bool {
			calls++
			return false
		})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 1, calls)
	})

	t.Run("no_match_for_another_user", func(t *testing.T) {
		f := filter
		f.Users = []string{"user:carl"}
		require.Empty(t, read(t, f))
	})

	t.Run("no_match_through_another_computed_relation", func(t *testing.T) {
		f := filter
		f.Object = "doc:4"
		require.Empty(t, read(t, f))
		f.Conditioned = false
		require.Empty(t, read(t, f))
	})
}
