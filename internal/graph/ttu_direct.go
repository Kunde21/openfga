package graph

import (
	"context"
	"slices"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// ttuDirectFilter returns the filter for resolving a weight-2 TTU in one datastore query. It
// returns false unless every parent type of the tupleset is a plain object type whose computed
// relation is directly assigned only, e.g. `define viewer: [user, user:*]`.
func ttuDirectFilter(typesys *typesystem.TypeSystem, req *ResolveCheckRequest, rewrite *openfgav1.Userset) (storage.TTUDirectFilter, bool) {
	tk := req.GetTupleKey()
	objectType := tuple.GetType(tk.GetObject())
	tuplesetRelation := rewrite.GetTupleToUserset().GetTupleset().GetRelation()
	computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

	parents, err := typesys.GetDirectlyRelatedUserTypes(objectType, tuplesetRelation)
	if err != nil {
		return storage.TTUDirectFilter{}, false
	}
	userType := tuple.GetType(tk.GetUser())
	users := []string{tk.GetUser()}
	wildcard := false
	parentTypes := make([]string, 0, len(parents))
	var unconditioned []storage.TTUDirectGrant
	conditioned := false
	for _, p := range parents {
		if p.GetRelationOrWildcard() != nil {
			return storage.TTUDirectFilter{}, false
		}
		rel, err := typesys.GetRelation(p.GetType(), computedRelation)
		if err != nil {
			continue // the parent type does not define the computed relation
		}
		if _, ok := rel.GetRewrite().GetUserset().(*openfgav1.Userset_This); !ok {
			return storage.TTUDirectFilter{}, false
		}
		public, err := typesys.IsPubliclyAssignable(typesystem.DirectRelationReference(p.GetType(), computedRelation), userType)
		if err != nil {
			return storage.TTUDirectFilter{}, false
		}
		wildcard = wildcard || public
		parentTypes = append(parentTypes, p.GetType())

		// The validator accepts a tuple with a condition only if a type restriction of its
		// relation names that condition, or if the relation has no type restrictions to check.
		grants, err := typesys.GetDirectlyRelatedUserTypes(p.GetType(), computedRelation)
		hasTypeInfo, infoErr := typesys.HasTypeInfo(p.GetType(), computedRelation)
		conditioned = conditioned || p.GetCondition() != "" || err != nil || infoErr != nil || !hasTypeInfo ||
			slices.ContainsFunc(grants, func(r *openfgav1.RelationReference) bool { return r.GetCondition() != "" })

		// The validator decides, on tuples without a condition, which matches the datastore may
		// treat as valid. It looks only at the types, so the object IDs are placeholders.
		parent := tuple.BuildObject(p.GetType(), "x")
		if validation.ValidateTupleForRead(typesys, tuple.NewTupleKey(tk.GetObject(), tuplesetRelation, parent)) != nil {
			continue
		}
		for _, user := range []string{tk.GetUser(), tuple.TypedPublicWildcard(userType)} {
			if validation.ValidateTupleForRead(typesys, tuple.NewTupleKey(parent, computedRelation, user)) == nil {
				unconditioned = append(unconditioned, storage.TTUDirectGrant{ParentType: p.GetType(), User: user})
			}
		}
	}
	if len(parentTypes) == 0 {
		return storage.TTUDirectFilter{}, false
	}
	if wildcard {
		users = append(users, tuple.TypedPublicWildcard(userType))
	}

	return storage.TTUDirectFilter{
		Object:           tk.GetObject(),
		TuplesetRelation: tuplesetRelation,
		ParentTypes:      parentTypes,
		ComputedRelation: computedRelation,
		Users:            users,
		Unconditioned:    unconditioned,
		Conditioned:      conditioned,
	}, true
}

// resolveTTUDirect resolves the TTU with one datastore query. It allows if both tuples of any
// match are valid for the model and meet their conditions, and stops reading at the first such
// match. As in weight2, a condition error is returned only when no match allows. When the
// datastore cannot answer, ok is false and the caller must use separate reads.
func resolveTTUDirect(ctx context.Context, req *ResolveCheckRequest, typesys *typesystem.TypeSystem, ds storage.RelationshipTupleReader, filter storage.TTUDirectFilter, opts storage.ReadOptions) (_ *ResolveCheckResponse, ok bool, _ error) {
	valid := validation.FilterInvalidTuples(typesys)
	conditionMet := checkutil.BuildTupleKeyConditionFilter(ctx, req.GetContext(), typesys)
	var lastErr error
	passes := func(tk *openfgav1.TupleKey) bool {
		if !valid(tk) {
			return false
		}
		met, err := conditionMet(tk)
		if err != nil {
			lastErr = err
		}
		return err == nil && met
	}
	allowed := false
	ok, err := storage.ReadTTUDirectMatches(ctx, ds, req.GetStoreID(), filter, opts, func(m storage.TTUDirectMatch) bool {
		allowed = passes(m.Tupleset) && passes(m.Parent)
		return !allowed
	})
	if err != nil || !ok {
		return nil, false, err
	}
	if !allowed && lastErr != nil {
		return nil, false, lastErr
	}
	return &ResolveCheckResponse{Allowed: allowed}, true, nil
}
