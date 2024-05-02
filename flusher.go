package kinesiswriter

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/shogo82148/go-retry"
)

type flusher struct {
	client       KinesisClient
	flushTimeout time.Duration
	streamARN    string
}

func (f *flusher) Flush(records [][]byte) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, f.flushTimeout)
	defer cancel()
	failedRecords, err := f.putRecords(ctx, records)
	if err != nil {
		return fmt.Errorf("failed to put records: %w", err)
	}
	if len(failedRecords) == 0 {
		return nil
	}
	retryPolicy := retry.Policy{
		MinDelay: 5 * time.Second,
		MaxDelay: f.flushTimeout,
		MaxCount: 3,
	}
	retrier := retryPolicy.Start(ctx)
	for retrier.Continue() {
		log.Printf("retry to put records: %d records are failed", len(failedRecords))
		var err error
		failedRecords, err = f.putRecords(ctx, failedRecords)
		if err != nil {
			return fmt.Errorf("failed to put records: %w", err)
		}
		if len(failedRecords) == 0 {
			break
		}
	}

	if len(failedRecords) > 0 {
		return fmt.Errorf("failed to put records: %d records are failed", len(failedRecords))
	}

	return nil
}

func (f *flusher) putRecords(ctx context.Context, records [][]byte) ([][]byte, error) {
	entries := make([]types.PutRecordsRequestEntry, len(records))
	for i, r := range records {
		key := rand.Int()
		entries[i] = types.PutRecordsRequestEntry{
			Data:         r,
			PartitionKey: aws.String(strconv.Itoa(key)),
		}
	}

	ret, err := f.client.PutRecords(ctx, &kinesis.PutRecordsInput{
		Records:   entries,
		StreamARN: aws.String(f.streamARN),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to put records: %w", err)
	}
	if ret.FailedRecordCount == nil || *ret.FailedRecordCount == 0 {
		return nil, nil
	}

	failedRecords := make([][]byte, 0, *ret.FailedRecordCount)
	for i, rr := range ret.Records {
		if rr.ErrorCode != nil {
			failedRecords = append(failedRecords, records[i])
		}
	}
	return failedRecords, nil
}
