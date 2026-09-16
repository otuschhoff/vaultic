package daemon

import (
	"reflect"
	"testing"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
)

func TestWriterStatusAttributionMapping(t *testing.T) {
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{
		Attribution: &vaulticdbv1.AttributionSnapshot{
			AdmissionWait: &vaulticdbv1.TimingSnapshot{
				Attempts:             3,
				Failures:             1,
				TotalUs:              17,
				MaxUs:                9,
				Completed:            3,
				Successes:            1,
				Cancellations:        1,
				Timeouts:             1,
				Active:               2,
				OldestActiveUs:       23,
				LatencyBucketUpperUs: []uint64{10, 100},
				LatencyBucketCounts:  []uint64{1, 2},
				ContentionAvailable:  true,
				Contentions:          4,
			},
			AdmissionLockHold:        &vaulticdbv1.TimingSnapshot{Completed: 7},
			EngineWriteBatches:       5,
			EngineBackpressureCount:  2,
			EngineCompactedBytes:     1024,
			EngineMemtableWriteBytes: 2048,
			EngineWalFlushBytes:      4096,
			EngineBackpressure: &vaulticdbv1.TimingSnapshot{
				Attempts:             9,
				Failures:             2,
				Completed:            8,
				Successes:            5,
				Cancellations:        1,
				Timeouts:             4,
				Active:               1,
				OldestActiveUs:       6000,
				TotalUs:              7000,
				MaxUs:                5000,
				LatencyBucketUpperUs: []uint64{1000, 5000},
				LatencyBucketCounts:  []uint64{3, 5},
			},
			EngineBatchWriteQueueDepth: 3,
			EngineBatchWriteQueue: &vaulticdbv1.TimingSnapshot{
				Completed:     11,
				Successes:     8,
				Failures:      2,
				Cancellations: 1,
			},
			EngineBatchWriteService: &vaulticdbv1.TimingSnapshot{
				Completed:     10,
				Successes:     7,
				Failures:      2,
				Cancellations: 1,
				Active:        1,
			},
			ObjectStoreMain: &vaulticdbv1.ObjectStoreRoleSnapshot{
				Put: &vaulticdbv1.ObjectOperationSnapshot{
					Timing:                    &vaulticdbv1.TimingSnapshot{Attempts: 2, Successes: 1, Active: 1},
					TransferredBytes:          512,
					TransferredBytesAvailable: true,
				},
			},
			ObjectStoreWal: &vaulticdbv1.ObjectStoreRoleSnapshot{
				MultipartPart: &vaulticdbv1.ObjectOperationSnapshot{
					Timing:                    &vaulticdbv1.TimingSnapshot{Failures: 1},
					TransferredBytesAvailable: true,
				},
			},
			ObjectStoreCoordination: &vaulticdbv1.ObjectStoreRoleSnapshot{
				Head:                        &vaulticdbv1.ObjectOperationSnapshot{Timing: &vaulticdbv1.TimingSnapshot{Completed: 3}},
				RetryDelayAvailable:         false,
				BackgroundPressureAvailable: false,
			},
		},
	})

	wait := status.Attribution.AdmissionWait
	if wait.Attempts != 3 || wait.Failures != 1 || wait.TotalUS != 17 || wait.MaxUS != 9 || wait.Completed != 3 || wait.Successes != 1 || wait.Cancellations != 1 || wait.Timeouts != 1 || wait.Active != 2 || wait.OldestActiveUS != 23 || !wait.ContentionAvailable || wait.Contentions != 4 {
		t.Fatalf("admission snapshot = %+v", status.Attribution.AdmissionWait)
	}
	if len(wait.LatencyBucketUpperUS) != 2 || wait.LatencyBucketUpperUS[1] != 100 || len(wait.LatencyBucketCounts) != 2 || wait.LatencyBucketCounts[1] != 2 {
		t.Fatalf("admission histogram = %+v", wait)
	}
	if status.Attribution.AdmissionLockHold.Completed != 7 {
		t.Fatalf("admission lock hold = %+v", status.Attribution.AdmissionLockHold)
	}
	if status.Attribution.EngineWriteBatches != 5 || status.Attribution.EngineBackpressureCount != 2 || status.Attribution.EngineCompactedBytes != 1024 || status.Attribution.EngineMemtableWriteBytes != 2048 || status.Attribution.EngineWALFlushBytes != 4096 {
		t.Fatalf("engine snapshot = %+v", status.Attribution)
	}
	backpressure := status.Attribution.EngineBackpressure
	if backpressure.Attempts != 9 || backpressure.Completed != 8 || backpressure.Successes != 5 || backpressure.Failures != 2 || backpressure.Cancellations != 1 || backpressure.Timeouts != 4 || backpressure.Active != 1 || backpressure.OldestActiveUS != 6000 || backpressure.TotalUS != 7000 || backpressure.MaxUS != 5000 || !reflect.DeepEqual(backpressure.LatencyBucketUpperUS, []uint64{1000, 5000}) || !reflect.DeepEqual(backpressure.LatencyBucketCounts, []uint64{3, 5}) {
		t.Fatalf("engine backpressure = %+v", backpressure)
	}
	if status.Attribution.EngineBatchQueueDepth != 3 || status.Attribution.EngineBatchQueue.Completed != 11 || status.Attribution.EngineBatchQueue.Successes != 8 || status.Attribution.EngineBatchQueue.Failures != 2 || status.Attribution.EngineBatchQueue.Cancellations != 1 {
		t.Fatalf("engine batch queue = %+v", status.Attribution)
	}
	if status.Attribution.EngineBatchService.Completed != 10 || status.Attribution.EngineBatchService.Successes != 7 || status.Attribution.EngineBatchService.Failures != 2 || status.Attribution.EngineBatchService.Cancellations != 1 || status.Attribution.EngineBatchService.Active != 1 {
		t.Fatalf("engine batch service = %+v", status.Attribution.EngineBatchService)
	}
	if status.Attribution.ObjectStoreMain.Put.Timing.Attempts != 2 || status.Attribution.ObjectStoreMain.Put.Timing.Active != 1 || status.Attribution.ObjectStoreMain.Put.TransferredBytes != 512 || !status.Attribution.ObjectStoreMain.Put.TransferredBytesAvailable {
		t.Fatalf("main object store = %+v", status.Attribution.ObjectStoreMain)
	}
	if status.Attribution.ObjectStoreWAL.MultipartPart.Timing.Failures != 1 || !status.Attribution.ObjectStoreWAL.MultipartPart.TransferredBytesAvailable || status.Attribution.ObjectStoreMain.MultipartPart.Timing.Failures != 0 {
		t.Fatalf("wal object store = %+v", status.Attribution.ObjectStoreWAL)
	}
	if status.Attribution.ObjectStoreCoordination.Head.Timing.Completed != 3 || status.Attribution.ObjectStoreCoordination.RetryDelayAvailable || status.Attribution.ObjectStoreCoordination.BackgroundPressureAvailable {
		t.Fatalf("coordination object store = %+v", status.Attribution.ObjectStoreCoordination)
	}

	legacy := writerStatus(&vaulticdbv1.WriterStatusResponse{})
	if !reflect.DeepEqual(legacy.Attribution, AttributionSnapshot{}) {
		t.Fatalf("legacy attribution = %+v", legacy.Attribution)
	}
}
