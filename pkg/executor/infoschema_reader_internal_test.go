// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type mockTableInfoIterator struct {
	tables        []*model.TableInfo
	next          int
	closeCount    *int
	destinations  *[]*model.TableInfo
	retainedBytes int64
	closed        bool
}

func (i *mockTableInfoIterator) NextInto(_ context.Context, destination *model.TableInfo) (*model.TableInfo, error) {
	if i.next >= len(i.tables) {
		return nil, nil
	}
	table := i.tables[i.next]
	i.next++
	if i.destinations != nil {
		*i.destinations = append(*i.destinations, destination)
	}
	*destination = *table
	return destination, nil
}

func (i *mockTableInfoIterator) Close() {
	if i.closed {
		return
	}
	i.closed = true
	(*i.closeCount)++
}

func (i *mockTableInfoIterator) RetainedMemory() int64 {
	if i.closed {
		return 0
	}
	return i.retainedBytes
}

func TestInfoSchemaColumnsUsesHugeRetriever(t *testing.T) {
	sctx := mock.NewContext()
	is := infoschema.MockInfoSchema(nil)
	builder := NewMockExecutorBuilderForTest(sctx, is, nil)
	plan := physicalop.PhysicalMemTable{
		DBName:    metadef.InformationSchemaName,
		Table:     &model.TableInfo{Name: ast.NewCIStr(infoschema.TableColumns)},
		Extractor: plannercore.NewInfoSchemaColumnsExtractor(),
	}.Init(sctx, nil, 0)
	plan.SetSchema(expression.NewSchema())
	reader := builder.Build(plan).(*MemTableReaderExec)
	require.IsType(t, &hugeMemTableRetriever{}, reader.retriever)
}

func TestHugeMemTableRetrieverKeepsTableInfoIteratorAcrossBatches(t *testing.T) {
	tables := make([]*model.TableInfo, 0, hugeMemTableBatchSize+1)
	for id := int64(1); id <= hugeMemTableBatchSize+1; id++ {
		tables = append(tables, &model.TableInfo{ID: id, Name: ast.NewCIStr("t")})
	}

	openCount := 0
	closeCount := 0
	destinations := make([]*model.TableInfo, 0, len(tables))
	tracker := memory.NewTracker(5, -1)
	retriever := &hugeMemTableRetriever{
		extractor:      plannercore.NewInfoSchemaColumnsExtractor(),
		dbs:            []ast.CIStr{ast.NewCIStr("test")},
		tableInfoBatch: newBoundedTableInfoBatch(tracker, 1<<20),
		memTracker:     tracker,
	}
	retriever.newTableInfoIter = func(_ context.Context, schema ast.CIStr, exclusiveStartTableID int64) (infoschema.TableInfoIterator, error) {
		require.Equal(t, "test", schema.L)
		require.Zero(t, exclusiveStartTableID)
		openCount++
		return &mockTableInfoIterator{
			tables:        tables,
			closeCount:    &closeCount,
			destinations:  &destinations,
			retainedBytes: 4096,
		}, nil
	}

	visited := make([]int64, 0, len(tables))
	retriever.tableInfoBatch.beginBatch()
	err := retriever.iterateTables(context.Background(), func(_ ast.CIStr, table *model.TableInfo) (bool, bool) {
		visited = append(visited, table.ID)
		return len(visited) < hugeMemTableBatchSize, true
	})
	require.NoError(t, err)
	retriever.tableInfoBatch.finishBatch()
	require.Equal(t, 1, openCount)
	require.Zero(t, closeCount)
	require.NotNil(t, retriever.tableInfoIter)
	require.Equal(t, int64(4096), retriever.tableInfoIterBytes)
	require.Len(t, destinations, hugeMemTableBatchSize)
	require.NotSame(t, destinations[0], destinations[1])
	firstDestination := destinations[0]

	retriever.tableInfoBatch.beginBatch()
	err = retriever.iterateTables(context.Background(), func(_ ast.CIStr, table *model.TableInfo) (bool, bool) {
		visited = append(visited, table.ID)
		return true, true
	})
	require.NoError(t, err)
	retriever.tableInfoBatch.finishBatch()
	require.Equal(t, 1, openCount)
	require.Equal(t, 1, closeCount)
	require.Nil(t, retriever.tableInfoIter)
	require.Zero(t, retriever.tableInfoIterBytes)
	require.Len(t, destinations, hugeMemTableBatchSize+1)
	require.Same(t, firstDestination, destinations[hugeMemTableBatchSize])
	require.Len(t, visited, hugeMemTableBatchSize+1)
	for i, id := range visited {
		require.Equal(t, int64(i+1), id)
	}
	retriever.tableInfoBatch.close()
	require.Zero(t, tracker.BytesConsumed())
}

func TestSetDataFromCheckConstraints(t *testing.T) {
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  ast.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: ast.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t2_c1"),
					Table:      ast.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: ast.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t3_c1"),
					Table:      ast.NewCIStr("t3"),
					ExprString: "id<10",
					State:      model.StateDeleteOnly,
				},
			},
			State: model.StatePublic,
		},
	}
	mockIs := infoschema.MockInfoSchema(tblInfos)
	mt := memtableRetriever{is: mockIs, extractor: &plannercore.InfoSchemaCheckConstraintsExtractor{}}
	sctx := defaultCtx()
	err := mt.setDataFromCheckConstraints(context.Background(), sctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(mt.rows))    // 1 row
	require.Equal(t, 4, len(mt.rows[0])) // 4 columns
	require.Equal(t, types.NewStringDatum("def"), mt.rows[0][0])
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("t2_c1"), mt.rows[0][2])
	require.Equal(t, types.NewStringDatum("(id<10)"), mt.rows[0][3])
}

func TestSetDataFromTiDBCheckConstraints(t *testing.T) {
	mt := memtableRetriever{}
	sctx := defaultCtx()
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  ast.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: ast.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t2_c1"),
					Table:      ast.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: ast.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t3_c1"),
					Table:      ast.NewCIStr("t3"),
					ExprString: "id<10",
					State:      model.StateDeleteOnly,
				},
			},
			State: model.StatePublic,
		},
	}
	mockIs := infoschema.MockInfoSchema(tblInfos)
	mt.is = mockIs
	mt.extractor = &plannercore.InfoSchemaTiDBCheckConstraintsExtractor{}
	err := mt.setDataFromTiDBCheckConstraints(context.Background(), sctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(mt.rows))    // 1 row
	require.Equal(t, 6, len(mt.rows[0])) // 6 columns
	require.Equal(t, types.NewStringDatum("def"), mt.rows[0][0])
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("t2_c1"), mt.rows[0][2])
	require.Equal(t, types.NewStringDatum("(id<10)"), mt.rows[0][3])
	require.Equal(t, types.NewStringDatum("t2"), mt.rows[0][4])
	require.Equal(t, types.NewIntDatum(2), mt.rows[0][5])
}

func TestSetDataFromKeywords(t *testing.T) {
	mt := memtableRetriever{}
	err := mt.setDataFromKeywords()
	require.NoError(t, err)
	require.Equal(t, types.NewStringDatum("ADD"), mt.rows[0][0]) // Keyword: ADD
	require.Equal(t, types.NewIntDatum(1), mt.rows[0][1])        // Reserved: true(1)
}
