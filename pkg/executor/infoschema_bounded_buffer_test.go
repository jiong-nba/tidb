// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func TestBoundedDatumRows(t *testing.T) {
	tableInfo := &model.TableInfo{Columns: []*model.ColumnInfo{
		{Offset: 0},
		{Offset: 1},
		{Offset: 2},
	}}
	outputColumns := []*model.ColumnInfo{tableInfo.Columns[0], tableInfo.Columns[2]}

	t.Run("reuse and release", func(t *testing.T) {
		tracker := memory.NewTracker(1, -1)
		rows := newBoundedDatumRows(tableInfo, outputColumns, tracker, 1<<20)
		rows.appendProjected("schema", 42, "table")
		batch := rows.rows()
		require.Len(t, batch, 1)
		require.Equal(t, "schema", batch[0][0].GetString())
		require.Equal(t, "table", batch[0][1].GetString())
		require.Equal(t, rows.ownedBytes, tracker.BytesConsumed())
		firstDatum := &batch[0][0]

		rows.beginBatch()
		require.Zero(t, rows.payloadBytes)
		typedRow := rows.appendTypedRow()
		typedRow.setString(0, "next_schema")
		typedRow.setInt(1, 42) // The middle full column is not projected.
		typedRow.setInt(2, 84)
		nextBatch := rows.rows()
		require.Same(t, firstDatum, &nextBatch[0][0])
		require.Equal(t, "next_schema", nextBatch[0][0].GetString())
		require.Equal(t, int64(84), nextBatch[0][1].GetInt64())
		require.Equal(t, rows.ownedBytes, tracker.BytesConsumed())

		rows.close()
		require.Zero(t, tracker.BytesConsumed())
	})

	t.Run("evict oversized retained capacity", func(t *testing.T) {
		tracker := memory.NewTracker(2, -1)
		rows := newBoundedDatumRows(tableInfo, outputColumns, tracker, 1)
		rows.appendProjected("schema", 42, "table")
		require.NotEmpty(t, rows.rows())
		require.Greater(t, rows.retainedBytes, rows.maxRetainedBytes)

		rows.beginBatch()
		require.Empty(t, rows.slots)
		require.Zero(t, rows.retainedBytes)
		require.Equal(t, rows.ownedBytes, tracker.BytesConsumed())

		rows.close()
		require.Zero(t, tracker.BytesConsumed())
	})

	t.Run("reuse table metadata only across batches", func(t *testing.T) {
		tracker := memory.NewTracker(3, -1)
		batch := newBoundedTableInfoBatch(tracker, 1<<20)
		batch.beginBatch()

		first := batch.nextDestination()
		first.Columns = []*model.ColumnInfo{{ID: 1}}
		batch.finishDecoded(true)
		second := batch.nextDestination()
		second.Columns = []*model.ColumnInfo{{ID: 2}}
		batch.finishDecoded(true)
		require.NotSame(t, first, second)
		batch.finishBatch()
		require.Positive(t, tracker.BytesConsumed())

		batch.beginBatch()
		reused := batch.nextDestination()
		require.Same(t, first, reused)
		batch.finishDecoded(true)
		batch.finishBatch()
		require.Len(t, batch.slots, 1)

		batch.close()
		require.Zero(t, tracker.BytesConsumed())
	})

	t.Run("evict oversized table metadata", func(t *testing.T) {
		tracker := memory.NewTracker(4, -1)
		batch := newBoundedTableInfoBatch(tracker, 1)
		batch.beginBatch()
		first := batch.nextDestination()
		first.Columns = []*model.ColumnInfo{{ID: 1}}
		batch.finishDecoded(true)
		batch.finishBatch()
		require.Greater(t, batch.retainedBytes, batch.maxRetainedBytes)

		batch.beginBatch()
		require.Empty(t, batch.slots)
		second := batch.nextDestination()
		require.NotSame(t, first, second)
		batch.finishDecoded(true)
		batch.finishBatch()

		batch.close()
		require.Zero(t, tracker.BytesConsumed())
	})

	t.Run("column type cache is bounded and tracked", func(t *testing.T) {
		tracker := memory.NewTracker(6, -1)
		retriever := &hugeMemTableRetriever{memTracker: tracker}
		for i := 0; i < hugeMemTableColumnTypeCacheMaxEntries; i++ {
			ft := types.NewFieldType(mysql.TypeVarchar)
			ft.SetFlen(i + 1)
			retriever.infoSchemaFieldTypeStrings(ft)
		}
		retainedAtLimit := tracker.BytesConsumed()
		require.Equal(t, hugeMemTableColumnTypeCacheMaxEntries, len(retriever.columnTypeCache))
		require.Positive(t, retainedAtLimit)

		for i := hugeMemTableColumnTypeCacheMaxEntries; i < 1024; i++ {
			ft := types.NewFieldType(mysql.TypeVarchar)
			ft.SetFlen(i + 1)
			retriever.infoSchemaFieldTypeStrings(ft)
		}
		require.Equal(t, hugeMemTableColumnTypeCacheMaxEntries, len(retriever.columnTypeCache))
		require.Equal(t, retainedAtLimit, tracker.BytesConsumed())

		require.NoError(t, retriever.close())
		require.Zero(t, tracker.BytesConsumed())
	})
}
