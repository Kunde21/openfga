package server

import (
	"context"
	"strconv"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// TestCheckTTUSkipsTuplesInvalidForModel checks that a weight-2 TTU ignores tuples that the
// current model no longer accepts, on Postgres (one-query TTU match) as on memory (separate reads).
func TestCheckTTUSkipsTuplesInvalidForModel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	pg := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	pgds, err := postgres.New(pg.GetConnectionURI(true), sqlcommon.NewConfig())
	require.NoError(t, err)
	t.Cleanup(pgds.Close)

	for name, ds := range map[string]storage.OpenFGADatastore{
		"memory":   memory.New(),
		"postgres": pgds,
	} {
		t.Run(name, func(t *testing.T) {
			s := MustNewServerWithOpts(WithDatastore(storagewrappers.NewContextWrapper(ds)))
			t.Cleanup(s.Close)

			ctx := context.Background()
			storeID := ulid.Make().String()
			writeModel := func(viewer string) string {
				m := parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type folder
						relations
							define viewer: ` + viewer + `
					type doc
						relations
							define parent: [folder]
							define can_view: viewer from parent
					condition cond(x: string) {
						x == "1"
					}`)
				resp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					TypeDefinitions: m.GetTypeDefinitions(),
					Conditions:      m.GetConditions(),
					SchemaVersion:   typesystem.SchemaVersion1_1,
				})
				require.NoError(t, err)
				return resp.GetAuthorizationModelId()
			}
			check := func(modelID string) bool {
				resp, err := s.Check(ctx, &openfgav1.CheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					TupleKey:             tuple.NewCheckRequestTupleKey("doc:1", "can_view", "user:anne"),
				})
				require.NoError(t, err)
				return resp.GetAllowed()
			}

			unconditional := writeModel("[user]")
			_, err := s.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: unconditional,
				Writes: &openfgav1.WriteRequestWrites{TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("doc:1", "parent", "folder:a"),
					tuple.NewTupleKey("folder:a", "viewer", "user:anne"),
				}},
			})
			require.NoError(t, err)
			require.True(t, check(unconditional))

			// folder:a#viewer@user:anne has no condition, so this model rejects it.
			conditional := writeModel("[user with cond]")
			require.False(t, check(conditional))
		})
	}
}

// TestCheckTTUFindsValidMatchAfterInvalidOne checks that invalid matches do not hide a valid one.
// The folder:a* tuples carry a condition that the second model no longer allows, and they sort
// before folder:b.
func TestCheckTTUFindsValidMatchAfterInvalidOne(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	pg := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	pgds, err := postgres.New(pg.GetConnectionURI(true), sqlcommon.NewConfig())
	require.NoError(t, err)
	t.Cleanup(pgds.Close)

	for name, ds := range map[string]storage.OpenFGADatastore{
		"memory":   memory.New(),
		"postgres": pgds,
	} {
		t.Run(name, func(t *testing.T) {
			s := MustNewServerWithOpts(WithDatastore(storagewrappers.NewContextWrapper(ds)))
			t.Cleanup(s.Close)

			ctx := context.Background()
			storeID := ulid.Make().String()
			var conditions string
			writeModel := func(viewer string) string {
				m := parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type folder
						relations
							define viewer: ` + viewer + `
					type doc
						relations
							define parent: [folder]
							define can_view: viewer from parent` + conditions)
				resp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					TypeDefinitions: m.GetTypeDefinitions(),
					Conditions:      m.GetConditions(),
					SchemaVersion:   typesystem.SchemaVersion1_1,
				})
				require.NoError(t, err)
				return resp.GetAuthorizationModelId()
			}

			conditions = `
					condition cond(x: string) {
						x == "1"
					}`
			withCondition := writeModel("[user, user with cond]")
			conditions = ""
			tuples := []*openfgav1.TupleKey{
				tuple.NewTupleKey("doc:1", "parent", "folder:b"),
				tuple.NewTupleKey("folder:b", "viewer", "user:anne"),
			}
			for i := range 10 {
				folder := "folder:a" + strconv.Itoa(i)
				tuples = append(tuples,
					tuple.NewTupleKey("doc:1", "parent", folder),
					tuple.NewTupleKeyWithCondition(folder, "viewer", "user:anne", "cond", nil))
			}
			_, err := s.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: withCondition,
				Writes:               &openfgav1.WriteRequestWrites{TupleKeys: tuples},
			})
			require.NoError(t, err)

			resp, err := s.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              storeID,
				AuthorizationModelId: writeModel("[user]"),
				TupleKey:             tuple.NewCheckRequestTupleKey("doc:1", "can_view", "user:anne"),
			})
			require.NoError(t, err)
			require.True(t, resp.GetAllowed())
		})
	}
}

// TestCheckTTUParentTypesWithDifferentRules checks that a tuple without a condition is judged by
// the rules of its own parent type. In the second model only folder still accepts user:anne and
// only group still accepts user:* without a condition.
func TestCheckTTUParentTypesWithDifferentRules(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	pg := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	pgds, err := postgres.New(pg.GetConnectionURI(true), sqlcommon.NewConfig())
	require.NoError(t, err)
	t.Cleanup(pgds.Close)

	for name, ds := range map[string]storage.OpenFGADatastore{
		"memory":   memory.New(),
		"postgres": pgds,
	} {
		t.Run(name, func(t *testing.T) {
			s := MustNewServerWithOpts(WithDatastore(storagewrappers.NewContextWrapper(ds)))
			t.Cleanup(s.Close)

			ctx := context.Background()
			storeID := ulid.Make().String()
			writeModel := func(folderViewer, groupViewer, parent string) string {
				m := parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type folder
						relations
							define viewer: ` + folderViewer + `
					type group
						relations
							define viewer: ` + groupViewer + `
					type doc
						relations
							define parent: ` + parent + `
							define can_view: viewer from parent
					condition cond(x: string) {
						x == "1"
					}`)
				resp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					TypeDefinitions: m.GetTypeDefinitions(),
					Conditions:      m.GetConditions(),
					SchemaVersion:   typesystem.SchemaVersion1_1,
				})
				require.NoError(t, err)
				return resp.GetAuthorizationModelId()
			}

			_, err := s.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: writeModel("[user, user:*]", "[user, user:*]", "[folder, group]"),
				Writes: &openfgav1.WriteRequestWrites{TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("doc:anne-folder", "parent", "folder:1"),
					tuple.NewTupleKey("folder:1", "viewer", "user:anne"),
					tuple.NewTupleKey("doc:anne-group", "parent", "group:1"),
					tuple.NewTupleKey("group:1", "viewer", "user:anne"),
					tuple.NewTupleKey("doc:public-folder", "parent", "folder:2"),
					tuple.NewTupleKey("folder:2", "viewer", "user:*"),
					tuple.NewTupleKey("doc:public-group", "parent", "group:2"),
					tuple.NewTupleKey("group:2", "viewer", "user:*"),
					tuple.NewTupleKey("doc:both", "parent", "group:2"),
				}},
			})
			require.NoError(t, err)

			// doc:both has one public group parent and 20 public folder parents. The folder
			// matches sort first.
			var folders []*openfgav1.TupleKey
			for i := range 20 {
				folder := "folder:b" + strconv.Itoa(i)
				folders = append(folders, tuple.NewTupleKey("doc:both", "parent", folder), tuple.NewTupleKey(folder, "viewer", "user:*"))
			}
			conditional := writeModel("[user, user:*, user with cond]", "[user, user:*]", "[folder, group]")
			_, err = s.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: conditional,
				Writes: &openfgav1.WriteRequestWrites{TupleKeys: append(folders,
					tuple.NewTupleKey("doc:met", "parent", "folder:3"),
					tuple.NewTupleKeyWithCondition("folder:3", "viewer", "user:anne", "cond",
						testutils.MustNewStruct(t, map[string]any{"x": "1"})),
				)},
			})
			require.NoError(t, err)

			check := func(modelID, object string) bool {
				resp, err := s.Check(ctx, &openfgav1.CheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					TupleKey:             tuple.NewCheckRequestTupleKey(object, "can_view", "user:anne"),
				})
				require.NoError(t, err)
				return resp.GetAllowed()
			}

			narrowed := writeModel("[user, user:* with cond]", "[user with cond, user:*]", "[folder, group]")
			require.True(t, check(narrowed, "doc:anne-folder"))
			require.False(t, check(narrowed, "doc:anne-group"))
			require.False(t, check(narrowed, "doc:public-folder"))
			require.True(t, check(narrowed, "doc:public-group"))
			// The valid group match is found among the folder matches that are no longer valid,
			// whether the model rejects their viewer tuples or their parent tuples.
			require.True(t, check(narrowed, "doc:both"))
			require.True(t, check(writeModel("[user, user:*]", "[user, user:*]", "[folder with cond, group]"), "doc:both"))

			// The condition is met, but a model that no longer accepts it on folder#viewer must deny.
			require.True(t, check(conditional, "doc:met"))
			require.False(t, check(narrowed, "doc:met"))

			// The tupleset relation now accepts a group parent only with a condition.
			tupleset := writeModel("[user, user:*]", "[user, user:*]", "[folder, group with cond]")
			require.True(t, check(tupleset, "doc:anne-folder"))
			require.False(t, check(tupleset, "doc:anne-group"))
			require.True(t, check(tupleset, "doc:public-folder"))
			require.False(t, check(tupleset, "doc:public-group"))
		})
	}
}

// TestCheckTTUConditions checks that a condition on either tuple of a match must be met.
func TestCheckTTUConditions(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	pg := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	pgds, err := postgres.New(pg.GetConnectionURI(true), sqlcommon.NewConfig())
	require.NoError(t, err)
	t.Cleanup(pgds.Close)

	for name, ds := range map[string]storage.OpenFGADatastore{
		"memory":   memory.New(),
		"postgres": pgds,
	} {
		t.Run(name, func(t *testing.T) {
			s := MustNewServerWithOpts(WithDatastore(storagewrappers.NewContextWrapper(ds)))
			t.Cleanup(s.Close)

			ctx := context.Background()
			storeID := ulid.Make().String()
			m := parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type folder
					relations
						define viewer: [user, user with cond]
				type doc
					relations
						define parent: [folder, folder with cond]
						define can_view: viewer from parent
				condition cond(x: string) {
					x == "1"
				}`)
			model, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: m.GetTypeDefinitions(),
				Conditions:      m.GetConditions(),
				SchemaVersion:   typesystem.SchemaVersion1_1,
			})
			require.NoError(t, err)

			met := testutils.MustNewStruct(t, map[string]any{"x": "1"})
			unmet := testutils.MustNewStruct(t, map[string]any{"x": "0"})
			_, err = s.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: model.GetAuthorizationModelId(),
				Writes: &openfgav1.WriteRequestWrites{TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition("folder:met", "viewer", "user:anne", "cond", met),
					tuple.NewTupleKeyWithCondition("folder:unmet", "viewer", "user:anne", "cond", unmet),
					tuple.NewTupleKey("folder:plain", "viewer", "user:anne"),
					tuple.NewTupleKey("doc:viewer-met", "parent", "folder:met"),
					tuple.NewTupleKey("doc:viewer-unmet", "parent", "folder:unmet"),
					tuple.NewTupleKeyWithCondition("doc:parent-met", "parent", "folder:plain", "cond", met),
					tuple.NewTupleKeyWithCondition("doc:parent-unmet", "parent", "folder:plain", "cond", unmet),
					tuple.NewTupleKey("doc:unmet-then-met", "parent", "folder:unmet"),
					tuple.NewTupleKey("doc:unmet-then-met", "parent", "folder:met"),
				}},
			})
			require.NoError(t, err)

			for object, allowed := range map[string]bool{
				"doc:viewer-met":     true,
				"doc:viewer-unmet":   false,
				"doc:parent-met":     true,
				"doc:parent-unmet":   false,
				"doc:unmet-then-met": true,
			} {
				resp, err := s.Check(ctx, &openfgav1.CheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: model.GetAuthorizationModelId(),
					TupleKey:             tuple.NewCheckRequestTupleKey(object, "can_view", "user:anne"),
				})
				require.NoError(t, err)
				require.Equal(t, allowed, resp.GetAllowed(), object)
			}

			// In this model only the parent tuple can carry a condition.
			m = parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type folder
					relations
						define viewer: [user]
				type doc
					relations
						define parent: [folder, folder with cond]
						define can_view: viewer from parent
				condition cond(x: string) {
					x == "1"
				}`)
			model, err = s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: m.GetTypeDefinitions(),
				Conditions:      m.GetConditions(),
				SchemaVersion:   typesystem.SchemaVersion1_1,
			})
			require.NoError(t, err)
			for object, allowed := range map[string]bool{"doc:parent-met": true, "doc:parent-unmet": false} {
				resp, err := s.Check(ctx, &openfgav1.CheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: model.GetAuthorizationModelId(),
					TupleKey:             tuple.NewCheckRequestTupleKey(object, "can_view", "user:anne"),
				})
				require.NoError(t, err)
				require.Equal(t, allowed, resp.GetAllowed(), object)
			}
		})
	}
}

// BenchmarkCheckTTUWeight2Postgres measures a weight-2 TTU check over a document with many parents.
// Compare with the one-query TTU match disabled to see its effect.
func BenchmarkCheckTTUWeight2Postgres(b *testing.B) {
	pg := storagefixtures.RunDatastoreTestContainer(b, "postgres")
	ds, err := postgres.New(pg.GetConnectionURI(true), sqlcommon.NewConfig(sqlcommon.WithMaxTuplesPerWrite(1000)))
	require.NoError(b, err)
	b.Cleanup(ds.Close)
	s := MustNewServerWithOpts(WithDatastore(storagewrappers.NewContextWrapper(ds)))
	b.Cleanup(s.Close)

	ctx := context.Background()
	storeID := ulid.Make().String()
	m := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user, user:*, user with cond]
		type doc
			relations
				define parent: [folder]
				define can_view: viewer from parent
		condition cond(x: string) {
			x == "1"
		}`)
	model, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID, TypeDefinitions: m.GetTypeDefinitions(), Conditions: m.GetConditions(),
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(b, err)

	const parents = 250
	var tuples []*openfgav1.TupleKey
	for i := range parents {
		folder := "folder:" + strconv.Itoa(i)
		tuples = append(tuples, tuple.NewTupleKey("doc:1", "parent", folder), tuple.NewTupleKey(folder, "viewer", "user:"+strconv.Itoa(i)))
	}
	tuples = append(tuples,
		tuple.NewTupleKey("folder:"+strconv.Itoa(parents-1), "viewer", "user:anne"),
		tuple.NewTupleKey("folder:"+strconv.Itoa(parents-1), "viewer", "user:wide"))
	// user:wide also views 5,000 folders that are not parents of doc:1. They are all parents of
	// doc:many, so that check has 5,000 unconditioned matches.
	for i := range 5000 {
		folder := "folder:x" + strconv.Itoa(i)
		tuples = append(tuples, tuple.NewTupleKey(folder, "viewer", "user:wide"), tuple.NewTupleKey("doc:many", "parent", folder))
	}
	// doc:cond has 2,000 matches that all carry a condition: met for user:met, not met for user:unmet.
	met := testutils.MustNewStruct(b, map[string]any{"x": "1"})
	unmet := testutils.MustNewStruct(b, map[string]any{"x": "0"})
	for i := range 2000 {
		folder := "folder:c" + strconv.Itoa(i)
		tuples = append(tuples,
			tuple.NewTupleKey("doc:cond", "parent", folder),
			tuple.NewTupleKeyWithCondition(folder, "viewer", "user:met", "cond", met),
			tuple.NewTupleKeyWithCondition(folder, "viewer", "user:unmet", "cond", unmet))
	}
	for start := 0; start < len(tuples); start += 100 {
		_, err := s.Write(ctx, &openfgav1.WriteRequest{
			StoreId: storeID, AuthorizationModelId: model.GetAuthorizationModelId(),
			Writes: &openfgav1.WriteRequestWrites{TupleKeys: tuples[start:min(start+100, len(tuples))]},
		})
		require.NoError(b, err)
	}

	// A model without conditions, for which the datastore does not look for conditioned matches.
	// It accepts the tuples already written.
	plain := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user, user:*]
		type doc
			relations
				define parent: [folder]
				define can_view: viewer from parent`)
	plainModel, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID, TypeDefinitions: plain.GetTypeDefinitions(), SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(b, err)

	for _, tc := range []struct {
		name, modelID, object, user string
		allowed                     bool
	}{
		{"doc:1/user:anne", model.GetAuthorizationModelId(), "doc:1", "user:anne", true},
		{"doc:1/user:nobody", model.GetAuthorizationModelId(), "doc:1", "user:nobody", false},
		{"doc:1/user:wide", model.GetAuthorizationModelId(), "doc:1", "user:wide", true},
		{"doc:many/user:wide", model.GetAuthorizationModelId(), "doc:many", "user:wide", true},
		{"doc:cond/user:met", model.GetAuthorizationModelId(), "doc:cond", "user:met", true},
		{"doc:cond/user:unmet", model.GetAuthorizationModelId(), "doc:cond", "user:unmet", false},
		{"no-conditions-model/doc:1/user:nobody", plainModel.GetAuthorizationModelId(), "doc:1", "user:nobody", false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				resp, err := s.Check(ctx, &openfgav1.CheckRequest{
					StoreId: storeID, AuthorizationModelId: tc.modelID,
					TupleKey: tuple.NewCheckRequestTupleKey(tc.object, "can_view", tc.user),
				})
				require.NoError(b, err)
				require.Equal(b, tc.allowed, resp.GetAllowed())
			}
		})
	}
}
