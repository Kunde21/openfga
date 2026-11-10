package postgres

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

var _ storage.TTUDirectMatcher = (*Datastore)(nil)

// ttuDirectPageSize is the number of parents that one query of ttuDirectPageQuery covers. A check
// that is allowed by a condition on its first page pays for the whole page, and that cost grows
// with the size. A check with many conditioned matches and none met pays one round trip per page;
// in a benchmark of 2,000 such matches that cost stopped falling between 250 and 500.
const ttuDirectPageSize = 250

// ttuDirectParents selects the tupleset tuples (object#tupleset@parent) by the primary key prefix
// (store, object_type, object_id, relation). The parent is in _user.
const ttuDirectParents = `store = $1 AND object_type = $2 AND object_id = $3 AND relation = $4
  AND user_type = 'user' AND split_part(_user, ':', 1) = ANY($7)`

// ttuDirectProbe joins each tupleset tuple t1 to the tuples t2 (parent#computed@user) of its
// parent. The join keys make t2 a probe of one key, so the cost grows with the number of parents
// and not with the number of tuples the user has. The parent's type cannot contain a colon, so
// the first colon splits a parent into type and ID.
//
// The object_id comparison is written twice so that the probe has a full key in whichever index
// the planner picks: the primary key compares object_id in the default collation and
// idx_user_lookup in the "C" collation. Equality is the same in both. With one comparison and no
// table statistics, the planner picked the index that could not use it.
//
// OFFSET 0 keeps the planner from flattening the subquery, which without table statistics makes
// it read every tuple of the user once per parent instead of probing.
//
// A condition name is stored as either NULL or the empty string when there is no condition.
const ttuDirectProbe = `
CROSS JOIN LATERAL (
  SELECT _user AS usr, COALESCE(condition_name, '') AS parent_cond, condition_context AS parent_ctx,
         object_type AS parent_type
  FROM tuple
  WHERE store = $1
    AND object_type = split_part(t1._user, ':', 1)
    AND object_id = substr(t1._user, strpos(t1._user, ':') + 1)
    AND object_id = substr(t1._user, strpos(t1._user, ':') + 1) COLLATE "C"
    AND relation = $5
    AND _user = ANY($6)
  OFFSET 0
) t2`

// ttuDirectAllowQuery returns one match without conditions that $8 lists as valid for the model,
// if there is one. That match is enough to allow, and the database stops looking when it finds it.
// The first column is 0 for a match.
const ttuDirectAllowQuery = `
SELECT 0, _user, '', NULL::bytea, usr, '', NULL::bytea
FROM tuple t1` + ttuDirectProbe + `
WHERE ` + ttuDirectParents + `
  AND COALESCE(condition_name, '') = '' AND parent_cond = '' AND parent_type || ' ' || usr = ANY($8)
LIMIT 1`

// ttuDirectPageQuery returns the matches of the next ttuDirectPageSize parents after the parent
// $9. If one of them has no conditions and $8 lists it as valid for the model, it is returned
// alone. Otherwise every match with a condition on either tuple is returned, because only the
// caller can evaluate those. If the page is full, a last row has 1 in the first column and the
// page's last parent in the second, for the next query.
var ttuDirectPageQuery = `
WITH p AS MATERIALIZED (
  SELECT _user, COALESCE(condition_name, '') AS ts_cond, condition_context AS ts_ctx
  FROM tuple
  WHERE ` + ttuDirectParents + ` AND _user > $9
  ORDER BY _user LIMIT ` + strconv.Itoa(ttuDirectPageSize) + `
), m AS MATERIALIZED (
  SELECT _user, ts_cond, ts_ctx, usr, parent_cond, parent_ctx,
         ts_cond = '' AND parent_cond = '' AS plain, parent_type || ' ' || usr = ANY($8) AS listed
  FROM p t1` + ttuDirectProbe + `
), allow AS MATERIALIZED (
  SELECT * FROM m WHERE plain AND listed LIMIT 1
)
SELECT 0, _user, ts_cond, ts_ctx, usr, parent_cond, parent_ctx FROM allow
UNION ALL
SELECT 0, _user, ts_cond, ts_ctx, usr, parent_cond, parent_ctx FROM m
WHERE NOT plain AND NOT EXISTS (SELECT 1 FROM allow)
UNION ALL
SELECT 1, max(_user), '', NULL, '', '', NULL FROM p HAVING count(*) = ` + strconv.Itoa(ttuDirectPageSize)

// ReadTTUDirectMatches see [storage.TTUDirectMatcher]. Each statement reads both levels of its
// matches, so the two tuples of a match come from one snapshot.
//
// When the model accepts no condition, one statement covers every parent. Otherwise each statement
// covers a page of parents, so that the caller can stop at a match whose conditions are met, and
// each page has its own snapshot.
func (s *Datastore) ReadTTUDirectMatches(ctx context.Context, store string, filter storage.TTUDirectFilter, options storage.ReadOptions, yield func(storage.TTUDirectMatch) bool) (bool, error) {
	ctx, span := startTrace(ctx, "ReadTTUDirectMatches")
	defer span.End()

	objectType, objectID := tupleUtils.SplitObject(filter.Object)
	unconditioned := make([]string, 0, len(filter.Unconditioned))
	for _, g := range filter.Unconditioned {
		unconditioned = append(unconditioned, g.ParentType+" "+g.User)
	}
	db := s.getPgxPool(options.Consistency.Preference)
	args := []any{store, objectType, objectID, filter.TuplesetRelation, filter.ComputedRelation, filter.Users, filter.ParentTypes, unconditioned}

	if !filter.Conditioned {
		_, err := ttuDirectRun(ctx, db, filter, yield, ttuDirectAllowQuery, args...)
		return err == nil, err
	}
	next, err := ttuDirectRun(ctx, db, filter, yield, ttuDirectPageQuery, append(args, "")...)
	for err == nil && next != "" {
		next, err = ttuDirectRun(ctx, db, filter, yield, ttuDirectPageQuery, append(args, next)...)
	}
	return err == nil, err
}

// ttuDirectRun runs one query and yields its matches. It returns the parent to start the next
// page after, or the empty string if there is no next page or yield stopped the read. The
// connection goes back to the pool before it returns.
func ttuDirectRun(ctx context.Context, db *pgxpool.Pool, filter storage.TTUDirectFilter, yield func(storage.TTUDirectMatch) bool, query string, args ...any) (next string, _ error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return "", HandleSQLError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var kind int
		var parent, tsCond, user, parentCond string
		var tsCtx, parentCtx []byte
		if err := rows.Scan(&kind, &parent, &tsCond, &tsCtx, &user, &parentCond, &parentCtx); err != nil {
			return "", HandleSQLError(err)
		}
		if kind == 1 {
			next = parent
			continue
		}
		tupleset, err := ttuDirectKey(filter.Object, filter.TuplesetRelation, parent, tsCond, tsCtx)
		if err != nil {
			return "", err
		}
		grant, err := ttuDirectKey(parent, filter.ComputedRelation, user, parentCond, parentCtx)
		if err != nil {
			return "", err
		}
		if !yield(storage.TTUDirectMatch{Tupleset: tupleset, Parent: grant}) {
			return "", nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", HandleSQLError(err)
	}
	return next, nil
}

func ttuDirectKey(object, relation, user, condition string, conditionContext []byte) (*openfgav1.TupleKey, error) {
	var c *structpb.Struct
	if conditionContext != nil {
		c = &structpb.Struct{}
		if err := proto.Unmarshal(conditionContext, c); err != nil {
			return nil, err
		}
	}
	return tupleUtils.NewTupleKeyWithCondition(object, relation, user, condition, c), nil
}
