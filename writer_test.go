package kinesiswriter_test

import (
	"bufio"
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	kinesiswriter "github.com/mackee/go-kinesis-writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testKinesisClient interface {
	kinesiswriter.KinesisClient
	Inputs() []*kinesis.PutRecordsInput
}

func TestWriter(t *testing.T) {
	type init struct {
		opts          []kinesiswriter.WriterConfigOption
		streamARN     string
		kinesisClient testKinesisClient
	}
	type input struct {
		records [][]byte
	}
	type expect struct {
		inputs []*kinesis.PutRecordsInput
		err    error
	}

	tests := []struct {
		name   string
		init   init
		input  input
		expect expect
	}{
		{
			name: "success: one record",
			init: init{
				streamARN:     "stream-arn",
				kinesisClient: &successKinesisClient{},
			},
			input: input{
				records: [][]byte{
					[]byte("record1"),
				},
			},
			expect: expect{
				inputs: []*kinesis.PutRecordsInput{
					{
						Records: []types.PutRecordsRequestEntry{
							{
								Data: []byte("record1"),
							},
						},
						StreamARN: aws.String("stream-arn"),
					},
				},
			},
		},
		{
			name: "success: multi line records",
			init: init{
				streamARN:     "stream-arn",
				kinesisClient: &successKinesisClient{},
			},
			input: input{
				records: [][]byte{
					[]byte("record1\nrecord2"),
				},
			},
			expect: expect{
				inputs: []*kinesis.PutRecordsInput{
					{
						Records: []types.PutRecordsRequestEntry{
							{
								Data: []byte("record1"),
							},
							{
								Data: []byte("record2"),
							},
						},
						StreamARN: aws.String("stream-arn"),
					},
				},
			},
		},
		{
			name: "success: WithSplitFunc",
			init: init{
				streamARN:     "stream-arn",
				kinesisClient: &successKinesisClient{},
				opts: []kinesiswriter.WriterConfigOption{
					kinesiswriter.WithSplitFunc(bufio.ScanWords),
				},
			},
			input: input{
				records: [][]byte{
					[]byte("hello world"),
				},
			},
			expect: expect{
				inputs: []*kinesis.PutRecordsInput{
					{
						Records: []types.PutRecordsRequestEntry{
							{
								Data: []byte("hello"),
							},
							{
								Data: []byte("world"),
							},
						},
						StreamARN: aws.String("stream-arn"),
					},
				},
			},
		},
		{
			name: "success: window",
			init: init{
				streamARN:     "stream-arn",
				kinesisClient: &successKinesisClient{},
				opts: []kinesiswriter.WriterConfigOption{
					kinesiswriter.WithBufferRecordWindow(3),
					kinesiswriter.WithBufferFlushInterval(10 * time.Millisecond),
				},
			},
			input: input{
				records: [][]byte{
					[]byte("record1"),
					[]byte("record2"),
					[]byte("record3"),
					[]byte("record4"),
				},
			},
			expect: expect{
				inputs: []*kinesis.PutRecordsInput{
					{
						Records: []types.PutRecordsRequestEntry{
							{Data: []byte("record1")},
							{Data: []byte("record2")},
							{Data: []byte("record3")},
						},
						StreamARN: aws.String("stream-arn"),
					},
					{
						Records: []types.PutRecordsRequestEntry{
							{Data: []byte("record4")},
						},
						StreamARN: aws.String("stream-arn"),
					},
				},
			},
		},
		{
			name: "success: partial failed putRecords",
			init: init{
				streamARN:     "stream-arn",
				kinesisClient: &partialFailedKinesisClient{},
			},
			input: input{
				records: [][]byte{
					[]byte("record1"),
					[]byte("record2"),
					[]byte("record3"),
					[]byte("record4"),
				},
			},
			expect: expect{
				inputs: []*kinesis.PutRecordsInput{
					{
						Records: []types.PutRecordsRequestEntry{
							{Data: []byte("record1")},
							{Data: []byte("record2")},
							{Data: []byte("record3")},
							{Data: []byte("record4")},
						},
						StreamARN: aws.String("stream-arn"),
					},
					{
						Records: []types.PutRecordsRequestEntry{
							{Data: []byte("record2")},
							{Data: []byte("record4")},
						},
						StreamARN: aws.String("stream-arn"),
					},
					{
						Records: []types.PutRecordsRequestEntry{
							{Data: []byte("record4")},
						},
						StreamARN: aws.String("stream-arn"),
					},
				},
			},
		},
	}
	opts := cmp.Options{
		cmpopts.IgnoreUnexported(kinesis.PutRecordsInput{}, types.PutRecordsRequestEntry{}),
		cmpopts.IgnoreFields(types.PutRecordsRequestEntry{}, "PartitionKey"),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			_opts := append(tt.init.opts, kinesiswriter.WithKinesisClient(tt.init.kinesisClient))
			writer, err := kinesiswriter.New(ctx, tt.init.streamARN, _opts...)
			require.NoError(t, err)
			for _, record := range tt.input.records {
				n, err := writer.Write(record)
				if err != nil {
					assert.ErrorContains(t, tt.expect.err, err.Error())
				} else {
					require.NoError(t, err)
					assert.Equal(t, len(record), n)
				}
			}
			time.Sleep(100 * time.Millisecond)
			require.NoError(t, writer.Close())
			if diff := cmp.Diff(tt.expect.inputs, tt.init.kinesisClient.Inputs(), opts...); diff != "" {
				t.Errorf("unexpected inputs (-want, +got):\n%s", diff)
			}
		})
	}
}

type successKinesisClient struct {
	inputs []*kinesis.PutRecordsInput
}

func (c *successKinesisClient) PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	c.inputs = append(c.inputs, params)
	entries := make([]types.PutRecordsResultEntry, len(params.Records))
	for i := range params.Records {
		entries[i] = types.PutRecordsResultEntry{
			SequenceNumber: aws.String(strconv.Itoa(rand.Int())),
			ShardId:        aws.String(strconv.Itoa(rand.Int())),
		}
	}

	return &kinesis.PutRecordsOutput{
		Records: entries,
	}, nil
}

func (c *successKinesisClient) Inputs() []*kinesis.PutRecordsInput {
	return c.inputs
}

type partialFailedKinesisClient struct {
	inputs []*kinesis.PutRecordsInput
}

func (c *partialFailedKinesisClient) PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	c.inputs = append(c.inputs, params)
	entries := make([]types.PutRecordsResultEntry, len(params.Records))
	var failedErrorCount int32
	for i := range params.Records {
		if i%2 != 0 {
			entries[i] = types.PutRecordsResultEntry{
				ErrorCode: aws.String("error"),
			}
			failedErrorCount++
		} else {
			entries[i] = types.PutRecordsResultEntry{
				SequenceNumber: aws.String(strconv.Itoa(rand.Int())),
				ShardId:        aws.String(strconv.Itoa(rand.Int())),
			}
		}
	}

	return &kinesis.PutRecordsOutput{
		Records:           entries,
		FailedRecordCount: aws.Int32(failedErrorCount),
	}, nil
}

func (c *partialFailedKinesisClient) Inputs() []*kinesis.PutRecordsInput {
	return c.inputs
}
