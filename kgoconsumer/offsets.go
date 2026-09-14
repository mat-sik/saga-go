package kgoconsumer

import "github.com/twmb/franz-go/pkg/kgo"

type processedEpochOffsetsTracker struct {
	processed        map[string]map[int32]map[int64]kgo.EpochOffset
	epochOffsetRange map[string]map[int32]epochOffsetRange
}

func newProcessedOffsetsTracker() *processedEpochOffsetsTracker {
	return &processedEpochOffsetsTracker{
		processed:        make(map[string]map[int32]map[int64]kgo.EpochOffset),
		epochOffsetRange: make(map[string]map[int32]epochOffsetRange),
	}
}

func (k *processedEpochOffsetsTracker) committableEpochOffsets() map[string]map[int32]kgo.EpochOffset {
	committable := make(map[string]map[int32]kgo.EpochOffset)
	for topic, processedFromPartition := range k.processed {
		for partition, epochOffsets := range processedFromPartition {
			partitionOffsetRange := k.epochOffsetRange[topic]

			minEpochOffset := partitionOffsetRange[partition].min
			maxEpochOffset := partitionOffsetRange[partition].max

			maxCommittableEpochOffset := minEpochOffset
			for maxCommittableOffset := maxCommittableEpochOffset.Offset; maxCommittableOffset <= maxEpochOffset.Offset; maxCommittableOffset++ {
				newMaxCommutableEpochOffset, ok := epochOffsets[maxCommittableOffset+1]
				if !ok {
					break
				}
				maxCommittableEpochOffset = newMaxCommutableEpochOffset
			}

			committableForPartition, ok := committable[topic]
			if !ok {
				committableForPartition = make(map[int32]kgo.EpochOffset)
				committable[topic] = committableForPartition
			}

			committableForPartition[partition] = maxCommittableEpochOffset
		}
	}
	return committable
}

func (k *processedEpochOffsetsTracker) registerAsProcessed(records ...*kgo.Record) {
	for _, record := range records {
		k.registerSingleAsProcessed(record)
	}
}

func (k *processedEpochOffsetsTracker) registerSingleAsProcessed(record *kgo.Record) {
	processedFromTopic, ok := k.processed[record.Topic]
	if !ok {
		processedFromTopic = make(map[int32]map[int64]kgo.EpochOffset)
		k.processed[record.Topic] = processedFromTopic
	}
	processedFromPartition, ok := processedFromTopic[record.Partition]
	if !ok {
		processedFromPartition = make(map[int64]kgo.EpochOffset)
		processedFromTopic[record.Partition] = processedFromPartition
	}

	offsetRangeFromTopic, ok := k.epochOffsetRange[record.Topic]
	if !ok {
		offsetRangeFromTopic = make(map[int32]epochOffsetRange)
		k.epochOffsetRange[record.Topic] = offsetRangeFromTopic
	}

	epochOffset := kgo.EpochOffset{
		Epoch:  record.LeaderEpoch,
		Offset: record.Offset + 1,
	}

	partitionOffsetRange := offsetRangeFromTopic[record.Partition]
	var zero kgo.EpochOffset
	if partitionOffsetRange.max == zero || epochOffset.Offset > partitionOffsetRange.max.Offset {
		partitionOffsetRange.max = epochOffset
	}
	if partitionOffsetRange.min == zero || epochOffset.Offset < partitionOffsetRange.min.Offset {
		partitionOffsetRange.min = epochOffset
	}

	offsetRangeFromTopic[record.Partition] = partitionOffsetRange
	processedFromPartition[epochOffset.Offset] = epochOffset
}

type epochOffsetRange struct {
	min kgo.EpochOffset
	max kgo.EpochOffset
}
