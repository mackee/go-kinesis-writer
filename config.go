package kinesiswriter

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

const (
	defaultBufferRecordWindow  = 10
	defaultBufferWriteTimeout  = 5 * time.Second
	defaultBufferFlushTimeout  = 30 * time.Second
	defaultBufferFlushInterval = 30 * time.Second
)

func defaultBufferErrorHandler(err error, elements [][]byte) {
	fmt.Fprintf(os.Stderr, "async-buffer: error %s", err)
	for i, elem := range elements {
		fmt.Fprintf(os.Stderr, "failed to write logs [%d]=%s", i, string(elem))
	}
}

type KinesisClient interface {
	PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
}

type writerConfig struct {
	splitFunc    bufio.SplitFunc
	bufferConfig *bufferConfig
	client       KinesisClient
}

type bufferConfig struct {
	recordWindow  uint32
	writeTimeout  time.Duration
	flushTimeout  time.Duration
	flushInterval time.Duration
	errorHandler  func(err error, elements [][]byte)
}

// WriterConfigOption is a configuration option for a Writer.
type WriterConfigOption func(*writerConfig)

// WithSeparator sets the separator between records.
func WithSplitFunc(fn bufio.SplitFunc) WriterConfigOption {
	return func(c *writerConfig) {
		c.splitFunc = fn
	}
}

// WithKinesisClient sets the Kinesis client.
func WithKinesisClient(client KinesisClient) WriterConfigOption {
	return func(c *writerConfig) {
		c.client = client
	}
}

// WithBufferRecordWindow sets the record window for the buffer.
func WithBufferRecordWindow(window uint32) WriterConfigOption {
	return func(c *writerConfig) {
		c.bufferConfig.recordWindow = window
	}
}

// WithBufferWriteTimeout sets the write timeout for the buffer.
func WithBufferWriteTimeout(timeout time.Duration) WriterConfigOption {
	return func(c *writerConfig) {
		c.bufferConfig.writeTimeout = timeout
	}
}

// WithBufferFlushTimeout sets the flush timeout for the buffer.
func WithBufferFlushTimeout(timeout time.Duration) WriterConfigOption {
	return func(c *writerConfig) {
		c.bufferConfig.flushTimeout = timeout
	}
}

// WithBufferFlushInterval sets the flush interval for the buffer.
func WithBufferFlushInterval(interval time.Duration) WriterConfigOption {
	return func(c *writerConfig) {
		c.bufferConfig.flushInterval = interval
	}
}

// WithBufferErrorHandler sets the error handler for the buffer.
func WithBufferErrorHandler(handler func(err error, elements [][]byte)) WriterConfigOption {
	return func(c *writerConfig) {
		c.bufferConfig.errorHandler = handler
	}
}
