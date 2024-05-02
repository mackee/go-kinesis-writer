# go-kinesis-writer

[![GoDoc](https://godoc.org/github.com/mackee/go-kinesis-writer?status.svg)](https://pkg.go.dev/github.com/mackee/go-kinesis-writer)

go-kinesis-writer is a library for Go that allows output from the [uber-go/zap](https://github.com/uber-go/zap) to be sent to [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/). By using this library, logs generated by zap can be streamed to Amazon Kinesis Data Streams in real-time, enabling the analysis and processing of large-scale log data.

## Features

* Easy Configuration: Add a few lines to your existing zap configuration to start outputting logs to Kinesis Data Streams.
* Asynchronous Transmission: Logs are sent asynchronously, ensuring that your application's performance is not hindered.
* Batch Processing: Logs are batch processed before being transmitted to Kinesis for efficient transport.

## Prerequisites

* Go 1.x
* An AWS account and configured Amazon Kinesis stream

## Installation

```bash
go get github.com/mackee/go-kinesis-writer
```

## Usage

### Basic Setup

This example is use [uber-go/zap](https://github.com/uber-go/zap) as the logger. First, set up your zap logger. Then, use go-kinesis-writer to redirect zap's output to Kinesis.

```go
package main

import (
    "github.com/mackee/go-kinesis-writer"
    "go.uber.org/zap"
)

func main() {
    // Setting up zap logger
    logger, _ := zap.NewProduction()

    ctx := context.Background()
    // Configuring Kinesis Writer
    kw, err := kinesiswriter.New(ctx, "your-kinesis-stream-arn")
    if err != nil {
        logger.Fatal("Failed to create kinesis writer", zap.Error(err))
    }

    // Integrating Kinesis Writer with zap
    logger = logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
        return zapcore.NewCore(
            zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
            kinesisWriter,
            zapcore.DebugLevel,
        )
    }))

    // Logging a test message
    logger.Info("This is a test log sent to Amazon Kinesis")
}
```

## Contributing

If you are interested in contributing to go-kinesis-writer, please feel free to submit pull requests or issues. Before contributing, please read the contribution guidelines.

## License

This project is licensed under the MIT License.

