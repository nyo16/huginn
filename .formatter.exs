# Used by "mix format".
#
# The generated protobuf module (lib/huginn/proto/) is excluded so it keeps the
# style emitted by protoc-gen-elixir and doesn't churn when the proto is
# regenerated. mix format has no native exclude, so we expand the globs and
# drop that directory.
[
  inputs:
    ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
    |> Enum.flat_map(&Path.wildcard(&1, match_dot: true))
    |> Enum.reject(&String.starts_with?(&1, "lib/huginn/proto/"))
]
