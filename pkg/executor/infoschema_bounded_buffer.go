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
	"unsafe"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
)

const (
	hugeMemTableBatchSize             = 1024
	hugeMemTableRetainedCapacityLimit = 16 << 20
	datumRowsTrackerFlushThreshold    = 64 << 10
)

type boundedDatumRows struct {
	slots             [][]types.Datum
	active            int
	projection        []int
	outputColumnCount int
	tracker           *memory.Tracker
	maxRetainedBytes  int64
	retainedBytes     int64
	payloadBytes      int64
	ownedBytes        int64
	reportedBytes     int64
}

func newBoundedDatumRows(
	table *model.TableInfo,
	columns []*model.ColumnInfo,
	tracker *memory.Tracker,
	maxRetainedBytes int64,
) *boundedDatumRows {
	projection := make([]int, len(table.Columns))
	for i := range projection {
		projection[i] = -1
	}
	for outputOffset, col := range columns {
		projection[col.Offset] = outputOffset
	}
	b := &boundedDatumRows{
		projection:        projection,
		outputColumnCount: len(columns),
		tracker:           tracker,
		maxRetainedBytes:  maxRetainedBytes,
	}
	b.adjustOwnedBytes(int64(cap(projection)) * size.SizeOfInt)
	b.syncTracker()
	return b
}

func (b *boundedDatumRows) adjustOwnedBytes(delta int64) {
	b.ownedBytes += delta
	if unreported := b.ownedBytes - b.reportedBytes; unreported >= datumRowsTrackerFlushThreshold || unreported <= -datumRowsTrackerFlushThreshold {
		b.syncTracker()
	}
}

func (b *boundedDatumRows) syncTracker() {
	if b.tracker != nil {
		b.tracker.Consume(b.ownedBytes - b.reportedBytes)
	}
	b.reportedBytes = b.ownedBytes
}

func (b *boundedDatumRows) beginBatch() {
	for i := 0; i < b.active; i++ {
		clear(b.slots[i])
	}
	b.active = 0
	b.adjustOwnedBytes(-b.payloadBytes)
	b.payloadBytes = 0
	if b.retainedBytes > b.maxRetainedBytes {
		clear(b.slots)
		b.slots = nil
		b.adjustOwnedBytes(-b.retainedBytes)
		b.retainedBytes = 0
	}
	b.syncTracker()
}

func (b *boundedDatumRows) appendEmptyRow() []types.Datum {
	if b.active == len(b.slots) {
		oldCap := cap(b.slots)
		b.slots = append(b.slots, nil)
		if cap(b.slots) != oldCap {
			delta := int64(cap(b.slots)-oldCap) * size.SizeOfSlice
			b.retainedBytes += delta
			b.adjustOwnedBytes(delta)
		}
	}

	row := b.slots[b.active]
	if cap(row) < b.outputColumnCount {
		oldBytes := int64(cap(row)) * types.EmptyDatumSize
		row = make([]types.Datum, b.outputColumnCount)
		b.slots[b.active] = row
		newBytes := int64(cap(row)) * types.EmptyDatumSize
		delta := newBytes - oldBytes
		b.retainedBytes += delta
		b.adjustOwnedBytes(delta)
	} else {
		row = row[:b.outputColumnCount]
		clear(row)
	}
	b.active++
	return row
}

func (b *boundedDatumRows) appendProjected(values ...any) {
	row := b.appendEmptyRow()

	for fullOffset, value := range values {
		if fullOffset >= len(b.projection) {
			break
		}
		outputOffset := b.projection[fullOffset]
		if outputOffset < 0 {
			continue
		}
		datum := types.NewDatum(value)
		row[outputOffset] = datum
		payloadBytes := datum.EstimatedMemUsage() - types.EmptyDatumSize
		b.payloadBytes += payloadBytes
		b.adjustOwnedBytes(payloadBytes)
	}
}

type projectedDatumRow struct {
	owner *boundedDatumRows
	row   []types.Datum
}

func (b *boundedDatumRows) appendTypedRow() projectedDatumRow {
	return projectedDatumRow{owner: b, row: b.appendEmptyRow()}
}

func (r projectedDatumRow) datum(fullOffset int) *types.Datum {
	if fullOffset < 0 || fullOffset >= len(r.owner.projection) {
		return nil
	}
	outputOffset := r.owner.projection[fullOffset]
	if outputOffset < 0 {
		return nil
	}
	return &r.row[outputOffset]
}

func (r projectedDatumRow) account(datum *types.Datum) {
	payloadBytes := datum.EstimatedMemUsage() - types.EmptyDatumSize
	r.owner.payloadBytes += payloadBytes
	r.owner.adjustOwnedBytes(payloadBytes)
}

func (r projectedDatumRow) setString(fullOffset int, value string) {
	datum := r.datum(fullOffset)
	if datum == nil {
		return
	}
	datum.SetString(value, mysql.DefaultCollationName)
	r.account(datum)
}

func (r projectedDatumRow) setInt(fullOffset, value int) {
	datum := r.datum(fullOffset)
	if datum == nil {
		return
	}
	datum.SetInt64(int64(value))
	r.account(datum)
}

func (b *boundedDatumRows) len() int {
	return b.active
}

func (b *boundedDatumRows) projects(fullOffset int) bool {
	return fullOffset >= 0 && fullOffset < len(b.projection) && b.projection[fullOffset] >= 0
}

func (b *boundedDatumRows) rows() [][]types.Datum {
	b.syncTracker()
	return b.slots[:b.active]
}

func (b *boundedDatumRows) close() {
	b.beginBatch()
	clear(b.slots)
	b.slots = nil
	b.adjustOwnedBytes(-b.retainedBytes)
	b.retainedBytes = 0
	b.adjustOwnedBytes(-int64(cap(b.projection)) * size.SizeOfInt)
	b.projection = nil
	b.syncTracker()
}

type tableInfoReuseSlot struct {
	tableInfo     *model.TableInfo
	retainedBytes int64
}

const (
	tableInfoObjectSize       = int64(unsafe.Sizeof(model.TableInfo{}))
	indexInfoObjectSize       = int64(unsafe.Sizeof(model.IndexInfo{}))
	indexColumnObjectSize     = int64(unsafe.Sizeof(model.IndexColumn{}))
	constraintInfoObjectSize  = int64(unsafe.Sizeof(model.ConstraintInfo{}))
	foreignKeyInfoObjectSize  = int64(unsafe.Sizeof(model.FKInfo{}))
	partitionInfoObjectSize   = int64(unsafe.Sizeof(model.PartitionInfo{}))
	partitionDefObjectSize    = int64(unsafe.Sizeof(model.PartitionDefinition{}))
	partitionStateObjectSize  = int64(unsafe.Sizeof(model.PartitionState{}))
	updateIndexInfoObjectSize = int64(unsafe.Sizeof(model.UpdateIndexInfo{}))
	storageRuleObjectSize     = int64(unsafe.Sizeof(model.StorageClassTransitRule{}))
	policyRefInfoObjectSize   = int64(unsafe.Sizeof(model.PolicyRefInfo{}))
	cistringObjectSize        = int64(unsafe.Sizeof(ast.CIStr{}))
	tableInfoReuseSlotSize    = int64(unsafe.Sizeof(tableInfoReuseSlot{}))
	tableInfoTrackerThreshold = 64 << 10
)

type boundedTableInfoBatch struct {
	slots            []tableInfoReuseSlot
	active           int
	tracker          *memory.Tracker
	maxRetainedBytes int64
	retainedBytes    int64
	reportedBytes    int64
}

func newBoundedTableInfoBatch(
	tracker *memory.Tracker,
	maxRetainedBytes int64,
) *boundedTableInfoBatch {
	return &boundedTableInfoBatch{
		tracker:          tracker,
		maxRetainedBytes: maxRetainedBytes,
	}
}

func (b *boundedTableInfoBatch) adjustRetainedBytes(delta int64) {
	b.retainedBytes += delta
	if unreported := b.retainedBytes - b.reportedBytes; unreported >= tableInfoTrackerThreshold || unreported <= -tableInfoTrackerThreshold {
		b.syncTracker()
	}
}

func (b *boundedTableInfoBatch) syncTracker() {
	if b.tracker != nil {
		b.tracker.Consume(b.retainedBytes - b.reportedBytes)
	}
	b.reportedBytes = b.retainedBytes
}

func (b *boundedTableInfoBatch) beginBatch() {
	if b.retainedBytes > b.maxRetainedBytes {
		b.releaseAll()
	}
	b.active = 0
	b.syncTracker()
}

func (b *boundedTableInfoBatch) nextDestination() *model.TableInfo {
	if b.active == len(b.slots) {
		oldCap := cap(b.slots)
		b.slots = append(b.slots, tableInfoReuseSlot{
			tableInfo:     &model.TableInfo{},
			retainedBytes: tableInfoObjectSize,
		})
		if cap(b.slots) != oldCap {
			b.adjustRetainedBytes(int64(cap(b.slots)-oldCap) * tableInfoReuseSlotSize)
		}
		b.adjustRetainedBytes(tableInfoObjectSize)
	}
	return b.slots[b.active].tableInfo
}

func (b *boundedTableInfoBatch) finishDecoded(retain bool) {
	slot := &b.slots[b.active]
	retainedBytes := reusableTableInfoMemoryUsage(slot.tableInfo)
	b.adjustRetainedBytes(retainedBytes - slot.retainedBytes)
	slot.retainedBytes = retainedBytes
	if retain {
		b.active++
	}
}

func (b *boundedTableInfoBatch) finishBatch() {
	for i := b.active; i < len(b.slots); i++ {
		b.adjustRetainedBytes(-b.slots[i].retainedBytes)
		b.slots[i] = tableInfoReuseSlot{}
	}
	b.slots = b.slots[:b.active]
	b.syncTracker()
}

func (b *boundedTableInfoBatch) releaseAll() {
	clear(b.slots)
	b.slots = nil
	b.active = 0
	b.adjustRetainedBytes(-b.retainedBytes)
	b.syncTracker()
}

func (b *boundedTableInfoBatch) close() {
	b.releaseAll()
}

func reusableTableInfoMemoryUsage(tableInfo *model.TableInfo) int64 {
	if tableInfo == nil {
		return 0
	}

	usage := tableInfoObjectSize
	columns := tableInfo.Columns[:cap(tableInfo.Columns)]
	usage += int64(cap(columns)) * size.SizeOfPointer
	for _, column := range columns {
		if column != nil {
			usage += model.EmptyColumnInfoSize
		}
	}

	indices := tableInfo.Indices[:cap(tableInfo.Indices)]
	usage += int64(cap(indices)) * size.SizeOfPointer
	for _, index := range indices {
		if index == nil {
			continue
		}
		usage += indexInfoObjectSize
		indexColumns := index.Columns[:cap(index.Columns)]
		usage += int64(cap(indexColumns)) * size.SizeOfPointer
		for _, column := range indexColumns {
			if column != nil {
				usage += indexColumnObjectSize
			}
		}
	}

	constraints := tableInfo.Constraints[:cap(tableInfo.Constraints)]
	usage += int64(cap(constraints)) * size.SizeOfPointer
	for _, constraint := range constraints {
		if constraint == nil {
			continue
		}
		usage += constraintInfoObjectSize
		usage += int64(cap(constraint.ConstraintCols)) * cistringObjectSize
	}

	foreignKeys := tableInfo.ForeignKeys[:cap(tableInfo.ForeignKeys)]
	usage += int64(cap(foreignKeys)) * size.SizeOfPointer
	for _, foreignKey := range foreignKeys {
		if foreignKey == nil {
			continue
		}
		usage += foreignKeyInfoObjectSize
		usage += int64(cap(foreignKey.RefCols)+cap(foreignKey.Cols)) * cistringObjectSize
	}
	usage += reusablePartitionInfoMemoryUsage(tableInfo.Partition)
	return usage
}

func reusablePartitionInfoMemoryUsage(partitionInfo *model.PartitionInfo) int64 {
	if partitionInfo == nil {
		return 0
	}

	usage := partitionInfoObjectSize + int64(len(partitionInfo.Expr)+len(partitionInfo.DDLExpr))
	usage += cistringSliceMemoryUsage(partitionInfo.Columns)
	usage += cistringSliceMemoryUsage(partitionInfo.DDLColumns)
	usage += partitionDefinitionSliceMemoryUsage(partitionInfo.Definitions)
	usage += partitionDefinitionSliceMemoryUsage(partitionInfo.AddingDefinitions)
	usage += partitionDefinitionSliceMemoryUsage(partitionInfo.DroppingDefinitions)
	usage += int64(cap(partitionInfo.NewPartitionIDs)+cap(partitionInfo.OriginalPartitionIDsOrder)) * size.SizeOfInt64
	usage += int64(cap(partitionInfo.States)) * partitionStateObjectSize

	updateIndexes := partitionInfo.DDLUpdateIndexes[:cap(partitionInfo.DDLUpdateIndexes)]
	usage += int64(cap(updateIndexes)) * updateIndexInfoObjectSize
	for _, updateIndex := range updateIndexes {
		usage += int64(len(updateIndex.IndexName))
	}
	if partitionInfo.DDLChangedIndex != nil {
		usage += size.SizeOfMap + int64(len(partitionInfo.DDLChangedIndex))*(size.SizeOfInt64+size.SizeOfBool)
	}
	return usage
}

func cistringSliceMemoryUsage(values []ast.CIStr) int64 {
	values = values[:cap(values)]
	usage := int64(cap(values)) * cistringObjectSize
	for _, value := range values {
		usage += int64(len(value.O) + len(value.L))
	}
	return usage
}

func partitionDefinitionSliceMemoryUsage(definitions []model.PartitionDefinition) int64 {
	definitions = definitions[:cap(definitions)]
	usage := int64(cap(definitions)) * partitionDefObjectSize
	for i := range definitions {
		definition := &definitions[i]
		usage += int64(len(definition.Name.O) + len(definition.Name.L) + len(definition.Comment) + len(definition.StorageClassTier))

		lessThan := definition.LessThan[:cap(definition.LessThan)]
		usage += int64(cap(lessThan)) * size.SizeOfString
		for _, value := range lessThan {
			usage += int64(len(value))
		}

		inValues := definition.InValues[:cap(definition.InValues)]
		usage += int64(cap(inValues)) * size.SizeOfSlice
		for _, values := range inValues {
			values = values[:cap(values)]
			usage += int64(cap(values)) * size.SizeOfString
			for _, value := range values {
				usage += int64(len(value))
			}
		}

		if definition.PlacementPolicyRef != nil {
			usage += policyRefInfoObjectSize
			usage += int64(len(definition.PlacementPolicyRef.Name.O) + len(definition.PlacementPolicyRef.Name.L))
		}

		transitions := definition.StorageClassTransitions[:cap(definition.StorageClassTransitions)]
		usage += int64(cap(transitions)) * storageRuleObjectSize
		for _, transition := range transitions {
			usage += int64(len(transition.Tier))
		}
	}
	return usage
}

type infoSchemaFieldTypeKey struct {
	tp      byte
	flag    uint
	flen    int
	decimal int
	charset string
	collate string
}

type infoSchemaFieldTypeStrings struct {
	dataType   string
	columnType string
}

const hugeMemTableColumnTypeCacheMaxEntries = 128

var infoSchemaFieldTypeCacheRetainedBytes = 2 * int64(hugeMemTableColumnTypeCacheMaxEntries) * (int64(unsafe.Sizeof(infoSchemaFieldTypeKey{})) +
	int64(unsafe.Sizeof(infoSchemaFieldTypeStrings{})) +
	2*size.SizeOfPointer)

func (e *hugeMemTableRetriever) adjustColumnTypeCacheBytes(delta int64) {
	e.columnTypeCacheBytes += delta
	if e.memTracker != nil {
		e.memTracker.Consume(delta)
	}
}

func (e *hugeMemTableRetriever) releaseColumnTypeCache() {
	e.columnTypeCache = nil
	e.adjustColumnTypeCacheBytes(-e.columnTypeCacheBytes)
}

func (e *hugeMemTableRetriever) infoSchemaFieldTypeStrings(ft *types.FieldType) infoSchemaFieldTypeStrings {
	build := func() infoSchemaFieldTypeStrings {
		colType := ft.GetType()
		if colType == mysql.TypeVarString {
			colType = mysql.TypeVarchar
		}
		return infoSchemaFieldTypeStrings{
			dataType:   types.TypeToStr(colType, ft.GetCharset()),
			columnType: ft.InfoSchemaStr(),
		}
	}
	if len(ft.GetElems()) > 0 || ft.IsArray() {
		return build()
	}
	key := infoSchemaFieldTypeKey{
		tp:      ft.GetType(),
		flag:    ft.GetFlag(),
		flen:    ft.GetFlen(),
		decimal: ft.GetDecimal(),
		charset: ft.GetCharset(),
		collate: ft.GetCollate(),
	}
	if cached, ok := e.columnTypeCache[key]; ok {
		return cached
	}
	result := build()
	if len(e.columnTypeCache) >= hugeMemTableColumnTypeCacheMaxEntries {
		return result
	}
	if e.columnTypeCache == nil {
		e.columnTypeCache = make(map[infoSchemaFieldTypeKey]infoSchemaFieldTypeStrings, hugeMemTableColumnTypeCacheMaxEntries)
		e.adjustColumnTypeCacheBytes(infoSchemaFieldTypeCacheRetainedBytes)
	}
	e.columnTypeCache[key] = result
	e.adjustColumnTypeCacheBytes(int64(
		len(key.charset) + len(key.collate) + len(result.dataType) + len(result.columnType),
	))
	return result
}
