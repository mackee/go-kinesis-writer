package kinesiswriter

import (
	"bufio"
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	buffer "github.com/woorui/async-buffer"
)

// Writer writes records to a Kinesis stream.
type Writer struct {
	ctx           context.Context
	config        *writerConfig
	kinesisBuffer *buffer.Buffer[[]byte]
}

// New creates a new Writer.
func New(ctx context.Context, streamARN string, opts ...WriterConfigOption) (*Writer, error) {
	conf := &writerConfig{
		splitFunc: bufio.ScanLines,
		bufferConfig: &bufferConfig{
			recordWindow:  defaultBufferRecordWindow,
			writeTimeout:  defaultBufferWriteTimeout,
			flushTimeout:  defaultBufferFlushTimeout,
			flushInterval: defaultBufferFlushInterval,
			errorHandler:  defaultBufferErrorHandler,
		},
	}

	for _, opt := range opts {
		opt(conf)
	}
	if conf.client == nil {
		awsConfig, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}
		conf.client = kinesis.NewFromConfig(awsConfig)
	}

	fl := &flusher{
		client:       conf.client,
		streamARN:    streamARN,
		flushTimeout: conf.bufferConfig.flushTimeout,
	}
	kb := buffer.New(fl, buffer.Option[[]byte]{
		Threshold:     conf.bufferConfig.recordWindow,
		WriteTimeout:  conf.bufferConfig.writeTimeout,
		FlushTimeout:  conf.bufferConfig.flushTimeout,
		FlushInterval: conf.bufferConfig.flushInterval,
		ErrHandler:    conf.bufferConfig.errorHandler,
	})

	return &Writer{
		config:        conf,
		kinesisBuffer: kb,
	}, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	scanner := bufio.NewScanner(bytes.NewReader(p))
	scanner.Split(w.config.splitFunc)

	for scanner.Scan() {
		line := scanner.Bytes()
		if _, err := w.kinesisBuffer.Write(line); err != nil {
			return 0, fmt.Errorf("failed to write to buffer: %w", err)
		}

	}
	return len(p), nil
}

func (w *Writer) Sync() error {
	w.kinesisBuffer.Flush()
	return nil
}

func (w *Writer) Close() error {
	if err := w.kinesisBuffer.Close(); err != nil {
		return fmt.Errorf("failed to close buffer: %w", err)
	}
	return nil
}
