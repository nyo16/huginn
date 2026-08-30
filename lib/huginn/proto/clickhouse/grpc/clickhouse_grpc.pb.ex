defmodule Clickhouse.Grpc.LogsLevel do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "clickhouse.grpc.LogsLevel",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :LOG_NONE, 0
  field :LOG_FATAL, 1
  field :LOG_CRITICAL, 2
  field :LOG_ERROR, 3
  field :LOG_WARNING, 4
  field :LOG_NOTICE, 5
  field :LOG_INFORMATION, 6
  field :LOG_DEBUG, 7
  field :LOG_TRACE, 8
  field :LOG_TEST, 9
end

defmodule Clickhouse.Grpc.ObsoleteTransportCompression.CompressionAlgorithm do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "clickhouse.grpc.ObsoleteTransportCompression.CompressionAlgorithm",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :NO_COMPRESSION, 0
  field :DEFLATE, 1
  field :GZIP, 2
  field :STREAM_GZIP, 3
end

defmodule Clickhouse.Grpc.ObsoleteTransportCompression.CompressionLevel do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "clickhouse.grpc.ObsoleteTransportCompression.CompressionLevel",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :COMPRESSION_NONE, 0
  field :COMPRESSION_LOW, 1
  field :COMPRESSION_MEDIUM, 2
  field :COMPRESSION_HIGH, 3
end

defmodule Clickhouse.Grpc.NameAndType do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.NameAndType",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :name, 1, type: :string
  field :type, 2, type: :string
end

defmodule Clickhouse.Grpc.ExternalTable.SettingsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.ExternalTable.SettingsEntry",
    map: true,
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Clickhouse.Grpc.ExternalTable do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.ExternalTable",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :name, 1, type: :string
  field :columns, 2, repeated: true, type: Clickhouse.Grpc.NameAndType
  field :data, 3, type: :bytes
  field :format, 4, type: :string
  field :compression_type, 6, type: :string, json_name: "compressionType"
  field :settings, 5, repeated: true, type: Clickhouse.Grpc.ExternalTable.SettingsEntry, map: true
end

defmodule Clickhouse.Grpc.ObsoleteTransportCompression do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.ObsoleteTransportCompression",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :algorithm, 1,
    type: Clickhouse.Grpc.ObsoleteTransportCompression.CompressionAlgorithm,
    enum: true

  field :level, 2, type: Clickhouse.Grpc.ObsoleteTransportCompression.CompressionLevel, enum: true
end

defmodule Clickhouse.Grpc.QueryInfo.SettingsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.QueryInfo.SettingsEntry",
    map: true,
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Clickhouse.Grpc.QueryInfo do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.QueryInfo",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :query, 1, type: :string
  field :query_id, 2, type: :string, json_name: "queryId"
  field :settings, 3, repeated: true, type: Clickhouse.Grpc.QueryInfo.SettingsEntry, map: true
  field :database, 4, type: :string
  field :input_data, 5, type: :bytes, json_name: "inputData"
  field :input_data_delimiter, 6, type: :bytes, json_name: "inputDataDelimiter"
  field :output_format, 7, type: :string, json_name: "outputFormat"
  field :send_output_columns, 24, type: :bool, json_name: "sendOutputColumns"

  field :external_tables, 8,
    repeated: true,
    type: Clickhouse.Grpc.ExternalTable,
    json_name: "externalTables"

  field :user_name, 9, type: :string, json_name: "userName"
  field :password, 10, type: :string
  field :quota, 11, type: :string
  field :jwt, 25, type: :string
  field :session_id, 12, type: :string, json_name: "sessionId"
  field :session_check, 13, type: :bool, json_name: "sessionCheck"
  field :session_timeout, 14, type: :uint32, json_name: "sessionTimeout"
  field :cancel, 15, type: :bool
  field :next_query_info, 16, type: :bool, json_name: "nextQueryInfo"
  field :input_compression_type, 20, type: :string, json_name: "inputCompressionType"
  field :output_compression_type, 21, type: :string, json_name: "outputCompressionType"
  field :output_compression_level, 19, type: :int32, json_name: "outputCompressionLevel"
  field :transport_compression_type, 22, type: :string, json_name: "transportCompressionType"
  field :transport_compression_level, 23, type: :int32, json_name: "transportCompressionLevel"

  field :obsolete_result_compression, 17,
    type: Clickhouse.Grpc.ObsoleteTransportCompression,
    json_name: "obsoleteResultCompression"

  field :obsolete_compression_type, 18, type: :string, json_name: "obsoleteCompressionType"
end

defmodule Clickhouse.Grpc.LogEntry do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.LogEntry",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :time, 1, type: :uint32
  field :time_microseconds, 2, type: :uint32, json_name: "timeMicroseconds"
  field :thread_id, 3, type: :uint64, json_name: "threadId"
  field :query_id, 4, type: :string, json_name: "queryId"
  field :level, 5, type: Clickhouse.Grpc.LogsLevel, enum: true
  field :source, 6, type: :string
  field :text, 7, type: :string
end

defmodule Clickhouse.Grpc.Progress do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.Progress",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :read_rows, 1, type: :uint64, json_name: "readRows"
  field :read_bytes, 2, type: :uint64, json_name: "readBytes"
  field :total_rows_to_read, 3, type: :uint64, json_name: "totalRowsToRead"
  field :written_rows, 4, type: :uint64, json_name: "writtenRows"
  field :written_bytes, 5, type: :uint64, json_name: "writtenBytes"
end

defmodule Clickhouse.Grpc.Stats do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.Stats",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :rows, 1, type: :uint64
  field :blocks, 2, type: :uint64
  field :allocated_bytes, 3, type: :uint64, json_name: "allocatedBytes"
  field :applied_limit, 4, type: :bool, json_name: "appliedLimit"
  field :rows_before_limit, 5, type: :uint64, json_name: "rowsBeforeLimit"
  field :applied_aggregation, 6, type: :bool, json_name: "appliedAggregation"
  field :rows_before_aggregation, 7, type: :uint64, json_name: "rowsBeforeAggregation"
end

defmodule Clickhouse.Grpc.Exception do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.Exception",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :code, 1, type: :int32
  field :name, 2, type: :string
  field :display_text, 3, type: :string, json_name: "displayText"
  field :stack_trace, 4, type: :string, json_name: "stackTrace"
end

defmodule Clickhouse.Grpc.Result do
  @moduledoc false

  use Protobuf,
    full_name: "clickhouse.grpc.Result",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :query_id, 9, type: :string, json_name: "queryId"
  field :time_zone, 10, type: :string, json_name: "timeZone"
  field :output_format, 11, type: :string, json_name: "outputFormat"

  field :output_columns, 12,
    repeated: true,
    type: Clickhouse.Grpc.NameAndType,
    json_name: "outputColumns"

  field :output, 1, type: :bytes
  field :totals, 2, type: :bytes
  field :extremes, 3, type: :bytes
  field :logs, 4, repeated: true, type: Clickhouse.Grpc.LogEntry
  field :progress, 5, type: Clickhouse.Grpc.Progress
  field :stats, 6, type: Clickhouse.Grpc.Stats
  field :exception, 7, type: Clickhouse.Grpc.Exception
  field :cancelled, 8, type: :bool
end

defmodule Clickhouse.Grpc.ClickHouse.Service do
  @moduledoc false

  use GRPC.Service, name: "clickhouse.grpc.ClickHouse", protoc_gen_elixir_version: "0.17.0"

  rpc :ExecuteQuery, Clickhouse.Grpc.QueryInfo, Clickhouse.Grpc.Result

  rpc :ExecuteQueryWithStreamInput, stream(Clickhouse.Grpc.QueryInfo), Clickhouse.Grpc.Result

  rpc :ExecuteQueryWithStreamOutput, Clickhouse.Grpc.QueryInfo, stream(Clickhouse.Grpc.Result)

  rpc :ExecuteQueryWithStreamIO, stream(Clickhouse.Grpc.QueryInfo), stream(Clickhouse.Grpc.Result)
end

defmodule Clickhouse.Grpc.ClickHouse.Stub do
  @moduledoc false

  use GRPC.Stub, service: Clickhouse.Grpc.ClickHouse.Service
end
