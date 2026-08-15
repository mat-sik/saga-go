package kafka

import (
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProcessedEpochOffsetsTracker(t *testing.T) {
	tests := []struct {
		name    string
		records []*kgo.Record
		want    map[string]map[int32]kgo.EpochOffset
	}{
		{
			name: "consume 0-89 then jump ahead to 98-99, gap stays uncommitted",
			records: append(
				consecutiveRecords("topic-1", 0, 0, 0, 90),
				newRecord("topic-1", 0, 0, 98),
				newRecord("topic-1", 0, 0, 99),
			),
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {
					0: {Epoch: 0, Offset: 90},
				},
			},
		},
		{
			name:    "gap filled by later registration advances the watermark",
			records: nil,
			want:    make(map[string]map[int32]kgo.EpochOffset),
		},
		{
			name: "exactly 2 contiguous records (even run length)",
			records: []*kgo.Record{
				newRecord("topic-1", 0, 0, 0),
				newRecord("topic-1", 0, 0, 1),
			},
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {0: {Epoch: 0, Offset: 2}},
			},
		},
		{
			name:    "exactly 4 contiguous records (even run length)",
			records: consecutiveRecords("topic-1", 0, 0, 0, 4),
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {0: {Epoch: 0, Offset: 4}},
			},
		},
		{
			name:    "large even-length run (100 records), mirrors real batch size",
			records: consecutiveRecords("topic-1", 0, 0, 0, 100),
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {0: {Epoch: 0, Offset: 100}},
			},
		},
		{
			name: "records registered out of order still resolve correct contiguous max",
			records: []*kgo.Record{
				newRecord("topic-1", 0, 0, 3),
				newRecord("topic-1", 0, 0, 0),
				newRecord("topic-1", 0, 0, 2),
				newRecord("topic-1", 0, 0, 1),
			},
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {0: {Epoch: 0, Offset: 4}},
			},
		},
		{
			name: "duplicate registration of same offset is idempotent",
			records: []*kgo.Record{
				newRecord("topic-1", 0, 0, 0),
				newRecord("topic-1", 0, 0, 1),
				newRecord("topic-1", 0, 0, 1),
				newRecord("topic-1", 0, 0, 2),
			},
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {0: {Epoch: 0, Offset: 3}},
			},
		},
		{
			name:    "first ever registered offset is non-zero",
			records: consecutiveRecords("topic-1", 0, 5, 50, 10),
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {0: {Epoch: 5, Offset: 60}},
			},
		},
		{
			name: "leader epoch changes but offsets stay contiguous",
			records: []*kgo.Record{
				newRecord("topic-1", 0, 1, 0),
				newRecord("topic-1", 0, 2, 1),
				newRecord("topic-1", 0, 2, 2),
			},
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {0: {Epoch: 2, Offset: 3}},
			},
		},
		{
			name: "single record with a preceding gap contributes nothing committable",
			records: []*kgo.Record{
				newRecord("topic-1", 0, 0, 5),
			},
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {0: {Epoch: 0, Offset: 6}},
			},
		},
		{
			name: "one partition gapped, sibling partition fully contiguous",
			records: append(
				[]*kgo.Record{
					newRecord("topic-1", 0, 0, 0),
					newRecord("topic-1", 0, 0, 2),
				},
				consecutiveRecords("topic-1", 1, 0, 0, 5)...,
			),
			want: map[string]map[int32]kgo.EpochOffset{
				"topic-1": {
					0: {Epoch: 0, Offset: 1},
					1: {Epoch: 0, Offset: 5},
				},
			},
		},
		{
			name:    "no records registered yields empty committable map",
			records: nil,
			want:    map[string]map[int32]kgo.EpochOffset{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Tracker := newProcessedOffsetsTracker()
			Tracker.registerAsProcessed(tt.records...)
			if got := Tracker.committableEpochOffsets(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got: %v want: %v", got, tt.want)
			}
		})
	}
}

func TestProcessedEpochOffsetsTracker_GapFilledAcrossCalls(t *testing.T) {
	Tracker := newProcessedOffsetsTracker()

	Tracker.registerAsProcessed(consecutiveRecords("topic-1", 0, 0, 0, 90)...)
	Tracker.registerAsProcessed(newRecord("topic-1", 0, 0, 98), newRecord("topic-1", 0, 0, 99))

	got := Tracker.committableEpochOffsets()
	want := map[string]map[int32]kgo.EpochOffset{
		"topic-1": {0: {Epoch: 0, Offset: 90}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("before gap filled: got %v want %v", got, want)
	}

	Tracker.registerAsProcessed(consecutiveRecords("topic-1", 0, 0, 90, 8)...)

	got = Tracker.committableEpochOffsets()
	want = map[string]map[int32]kgo.EpochOffset{
		"topic-1": {0: {Epoch: 0, Offset: 100}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("after gap filled: got %v want %v", got, want)
	}
}

func consecutiveRecords(topic string, partition, epoch int32, startOffset int64, n int) []*kgo.Record {
	records := make([]*kgo.Record, n)
	for i := range n {
		records[i] = newRecord(topic, partition, epoch, startOffset+int64(i))
	}
	return records
}

func newRecord(topic string, partition int32, epoch int32, offset int64) *kgo.Record {
	return &kgo.Record{
		Topic:       topic,
		Partition:   partition,
		LeaderEpoch: epoch,
		Offset:      offset,
	}
}
