#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "open3"
require "time"

CHECKPOINT_PREFIX = "__CSV_BENCHMARK_CHECKPOINT__"
MEMORY_CURRENT_PATHS = [
  "/sys/fs/cgroup/memory.current",
  "/sys/fs/cgroup/memory/memory.usage_in_bytes"
].freeze
MEMORY_PEAK_PATHS = [
  "/sys/fs/cgroup/memory.peak",
  "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"
].freeze
MEMORY_STAT_PATHS = [
  "/sys/fs/cgroup/memory.stat",
  "/sys/fs/cgroup/memory/memory.stat"
].freeze

def read_integer_file(paths)
  path = paths.find { |candidate| File.file?(candidate) && File.readable?(candidate) }
  return nil unless path

  value = File.read(path).strip
  return nil if value.empty? || value == "max"

  Integer(value)
rescue Errno::ENOENT, ArgumentError
  nil
end

def current_memory_bytes
  read_integer_file(MEMORY_CURRENT_PATHS)
end

def peak_memory_bytes
  read_integer_file(MEMORY_PEAK_PATHS)
end

def read_memory_stat
  path = MEMORY_STAT_PATHS.find { |candidate| File.file?(candidate) && File.readable?(candidate) }
  return {} unless path

  File.readlines(path).each_with_object({}) do |line, stat|
    key, value = line.split
    stat[key] = Integer(value) if key && value
  end
rescue Errno::ENOENT, ArgumentError
  {}
end

def memory_snapshot
  stat = read_memory_stat

  {
    "total_bytes" => current_memory_bytes,
    "anon_bytes" => stat["anon"] || stat["total_rss"] || stat["rss"],
    "file_bytes" => stat["file"] || stat["total_cache"] || stat["cache"]
  }.compact
end

def primary_memory_bytes(snapshot)
  snapshot["anon_bytes"] || snapshot["total_bytes"]
end

def parse_result(stdout)
  stdout.lines.reverse_each do |line|
    stripped = line.strip
    next if stripped.empty?

    return JSON.parse(stripped)
  rescue JSON::ParserError
    next
  end

  nil
end

if ARGV.empty?
  warn "Usage: #{$PROGRAM_NAME} COMMAND [ARGS...]"
  exit 2
end

started_at = Time.now.utc
started_monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
checkpoints = {}
checkpoint_details = {}
observed_peak_total = nil
observed_peak_anon = nil
polling = true

poller = Thread.new do
  while polling
    snapshot = memory_snapshot
    total = snapshot["total_bytes"]
    anon = snapshot["anon_bytes"]
    observed_peak_total = [observed_peak_total, total].compact.max
    observed_peak_anon = [observed_peak_anon, anon].compact.max
    sleep 0.01
  end
end

stdout_text = +""
stderr_text = +""
wait_thread = nil
status = nil

Open3.popen3(*ARGV) do |stdin, stdout, stderr, thread|
  stdin.close
  wait_thread = thread

  stdout_reader = Thread.new do
    stdout.each_line do |line|
      stdout_text << line
    end
  end

  stderr_reader = Thread.new do
    stderr.each_line do |line|
      stripped = line.strip

      if stripped.start_with?(CHECKPOINT_PREFIX)
        name = stripped.delete_prefix(CHECKPOINT_PREFIX).strip
        snapshot = memory_snapshot
        checkpoints[name] = primary_memory_bytes(snapshot)
        checkpoint_details[name] = snapshot
      else
        stderr_text << line
      end
    end
  end

  stdout_reader.join
  stderr_reader.join
  status = wait_thread.value
end

polling = false
poller.join

wall_time_seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_monotonic
peak_total = [observed_peak_total, peak_memory_bytes].compact.max
peak_anon = observed_peak_anon
peak = peak_anon || peak_total
result = parse_result(stdout_text)

envelope = {
  "started_at" => started_at.iso8601(6),
  "command" => ARGV,
  "exit_status" => status&.exitstatus,
  "signaled" => status&.signaled?,
  "termsig" => status&.termsig,
  "wall_time_seconds" => wall_time_seconds,
  "memory" => {
    "peak_bytes" => peak,
    "peak_anon_bytes" => peak_anon,
    "peak_total_bytes" => peak_total,
    "checkpoints" => checkpoints,
    "checkpoint_details" => checkpoint_details
  },
  "result" => result,
  "stdout_bytes" => stdout_text.bytesize,
  "stderr_bytes" => stderr_text.bytesize
}

envelope["stdout"] = stdout_text unless result
envelope["stderr"] = stderr_text unless stderr_text.empty?

puts JSON.generate(envelope)
exit(status&.exitstatus || 1)
