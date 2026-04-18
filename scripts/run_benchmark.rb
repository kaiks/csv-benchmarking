#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "optparse"
require "time"

TASKS = %w[
  read
  count_high_balances
  count_duplicate_rows
  total_account_balance
  count_invalid_dob_rows
  invalid_dob_indexes
  all
].freeze

DEFAULT_FILES = {
  "data/people_1k.csv" => 1_000,
  "data/people_10k.csv" => 100,
  "data/people_1m.csv" => 100
}.freeze

DEFAULT_MANIFEST = "implementations/manifest.json"
DEFAULT_REFERENCE_IMPLEMENTATION = "ruby_csv_default"

def parse_file_spec(value)
  path, iterations = value.split(":", 2)
  iterations = iterations ? Integer(iterations) : nil

  [path, iterations]
end

def load_manifest(path)
  return {} unless File.file?(path)

  JSON.parse(File.read(path)).each_with_object({}) do |entry, manifest|
    manifest.fetch(entry.fetch("name")) { manifest[entry.fetch("name")] = entry }
  end
end

def implementation_spec(value, manifest)
  spec = manifest[value]
  return spec.dup if spec

  matched = manifest.values.find { |entry| entry["script"] == value }
  return matched.dup if matched

  {
    "name" => implementation_name(value),
    "script" => value,
    "gemfile" => nil
  }
end

def validate_implementation_spec!(spec)
  abort "Implementation script not found for #{spec["name"]}: #{spec["script"]}" unless File.file?(spec["script"])

  gemfile = spec["gemfile"]
  abort "Implementation Gemfile not found for #{spec["name"]}: #{gemfile}" if gemfile && !File.file?(gemfile)
end

def docker_image_exists?(image)
  _stdout, _stderr, status = Open3.capture3("docker", "image", "inspect", image)
  status.success?
end

def run_command!(command)
  stdout, stderr, status = Open3.capture3(*command)
  return stdout if status.success?

  warn stderr unless stderr.empty?
  abort "Command failed: #{command.join(" ")}"
end

def ensure_docker_image!(image, dockerfile, build)
  unless build
    abort "Docker image not found: #{image}. Re-run without --no-build first." unless docker_image_exists?(image)
    return
  end

  run_command!(["docker", "build", "-f", dockerfile, "-t", image, "."])
end

def percentile(values, percentile)
  return nil if values.empty?

  sorted = values.sort
  index = ((percentile / 100.0) * (sorted.length - 1)).round
  sorted.fetch(index)
end

def mean(values)
  return nil if values.empty?

  values.sum / values.length.to_f
end

def bytes_to_mib(bytes)
  return nil unless bytes

  bytes / 1024.0 / 1024.0
end

def summarize(records)
  records
    .group_by { |record| [record["implementation"], record["file"], record["task"]] }
    .map do |(implementation, file, task), group|
      successful = group.select { |record| record["exit_status"]&.zero? }
      durations = successful.map { |record| record["wall_time_seconds"] }
      peaks = successful.filter_map { |record| record.dig("memory", "peak_bytes") }
      post_reads = successful.filter_map { |record| record.dig("memory", "checkpoints", "post_file_read") }
      correct = group.count { |record| record["correct"] == true }
      incorrect = group.count { |record| record["correct"] == false }

      {
        "implementation" => implementation,
        "file" => file,
        "task" => task,
        "runs" => group.length,
        "successful_runs" => successful.length,
        "failed_runs" => group.length - successful.length,
        "correct_runs" => correct,
        "incorrect_runs" => incorrect,
        "wall_time_seconds" => {
          "mean" => mean(durations),
          "min" => durations.min,
          "p50" => percentile(durations, 50),
          "p95" => percentile(durations, 95),
          "max" => durations.max
        },
        "peak_memory_mib" => {
          "mean" => bytes_to_mib(mean(peaks)),
          "max" => bytes_to_mib(peaks.max)
        },
        "post_file_read_memory_mib" => {
          "mean" => bytes_to_mib(mean(post_reads)),
          "max" => bytes_to_mib(post_reads.max)
        }
      }
    end
end

def implementation_name(path)
  File.basename(path, File.extname(path))
end

def implementation_command(spec, file, task, balance_threshold)
  command =
    if spec["gemfile"]
      ["env", "BUNDLE_GEMFILE=#{spec["gemfile"]}", "bundle", "exec", "ruby", spec["script"]]
    else
      ["ruby", spec["script"]]
    end

  command + [
    "--file", file,
    "--task", task,
    "--balance-threshold", balance_threshold.to_s
  ]
end

def canonical_result(task, result)
  return nil unless result

  case task
  when "read"
    {
      "row_count" => result["row_count"]
    }
  when "count_high_balances"
    {
      "row_count" => result["row_count"],
      "threshold_cents" => result["threshold_cents"],
      "count" => result["count"]
    }
  when "count_duplicate_rows"
    {
      "row_count" => result["row_count"],
      "unique_rows" => result["unique_rows"],
      "duplicate_rows" => result["duplicate_rows"]
    }
  when "total_account_balance"
    {
      "row_count" => result["row_count"],
      "total_account_balance_cents" => result["total_account_balance_cents"]
    }
  when "count_invalid_dob_rows"
    {
      "row_count" => result["row_count"],
      "invalid_dob_rows" => result["invalid_dob_rows"]
    }
  when "invalid_dob_indexes"
    {
      "row_count" => result["row_count"],
      "invalid_dob_indexes" => result["invalid_dob_indexes"]
    }
  when "all"
    {
      "row_count" => result["row_count"],
      "threshold_cents" => result["threshold_cents"],
      "count_high_balances" => result["count_high_balances"],
      "unique_rows" => result["unique_rows"],
      "duplicate_rows" => result["duplicate_rows"],
      "total_account_balance_cents" => result["total_account_balance_cents"],
      "invalid_dob_rows" => result["invalid_dob_rows"],
      "invalid_dob_indexes" => result["invalid_dob_indexes"]
    }
  end
end

def correctness_for(task, actual_result, expected_result)
  actual = canonical_result(task, actual_result)
  expected = canonical_result(task, expected_result)
  correct = actual == expected

  payload = { "correct" => correct }
  payload["expected"] = expected unless correct
  payload["actual"] = actual unless correct
  payload
end

def run_docker_measurement(image, repo_root, spec, file, task, balance_threshold)
  command = [
    "docker", "run", "--rm",
    "-v", "#{repo_root}:/app",
    "-w", "/app",
    image,
    "ruby", "scripts/container_measure.rb",
    *implementation_command(spec, file, task, balance_threshold)
  ]

  stdout, stderr, status = Open3.capture3(*command)
  envelope = JSON.parse(stdout.lines.last || "{}")
  envelope["docker_exit_status"] = status.exitstatus
  envelope["docker_stderr"] = stderr unless stderr.empty?
  envelope
rescue JSON::ParserError => error
  {
    "exit_status" => status&.exitstatus || 1,
    "docker_exit_status" => status&.exitstatus,
    "wall_time_seconds" => nil,
    "memory" => {},
    "result" => nil,
    "stdout" => stdout,
    "stderr" => [stderr, error.message].compact.join("\n")
  }
end

options = {
  implementations: [],
  manifest: DEFAULT_MANIFEST,
  reference_implementation: DEFAULT_REFERENCE_IMPLEMENTATION,
  files: [],
  tasks: TASKS,
  docker_image: "csv-benchmarking:latest",
  dockerfile: "Dockerfile",
  build: true,
  output: "benchmarks/results/benchmark_#{Time.now.utc.strftime("%Y%m%d_%H%M%S")}.jsonl",
  expected_output: nil,
  warmups: 0,
  balance_threshold: 10_000.0,
  skip_missing: false,
  limit_runs: nil,
  verify: true
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options]"

  opts.on("--implementation NAME_OR_PATH", "Implementation name or script path; repeatable") do |value|
    options[:implementations] << value
  end

  opts.on("--manifest PATH", "Implementation manifest, default: #{options[:manifest]}") do |value|
    options[:manifest] = value
  end

  opts.on("--reference-implementation NAME_OR_PATH", "Correctness reference, default: #{options[:reference_implementation]}") do |value|
    options[:reference_implementation] = value
  end

  opts.on("--file PATH[:ITERATIONS]", "File to benchmark; repeatable") do |value|
    options[:files] << parse_file_spec(value)
  end

  opts.on("--tasks LIST", "Comma-separated tasks, default: #{TASKS.join(",")}") do |value|
    options[:tasks] = value.split(",")
  end

  opts.on("--iterations N", Integer, "Override iterations for every file") do |value|
    options[:iterations] = value
  end

  opts.on("--warmups N", Integer, "Warmup runs per file/task") do |value|
    options[:warmups] = value
  end

  opts.on("--balance-threshold N", Float, "Default: #{options[:balance_threshold]}") do |value|
    options[:balance_threshold] = value
  end

  opts.on("--docker-image NAME", "Default: #{options[:docker_image]}") do |value|
    options[:docker_image] = value
  end

  opts.on("--dockerfile PATH", "Default: #{options[:dockerfile]}") do |value|
    options[:dockerfile] = value
  end

  opts.on("--[no-]build", "Build Docker image before running") do |value|
    options[:build] = value
  end

  opts.on("--output PATH", "JSONL output path") do |value|
    options[:output] = value
  end

  opts.on("--expected-output PATH", "Expected-result JSON path") do |value|
    options[:expected_output] = value
  end

  opts.on("--[no-]verify", "Check each result against the reference implementation") do |value|
    options[:verify] = value
  end

  opts.on("--skip-missing", "Skip missing CSV files") do
    options[:skip_missing] = true
  end

  opts.on("--limit-runs N", Integer, "Stop after N measured runs; useful for smoke tests") do |value|
    options[:limit_runs] = value
  end
end

parser.parse!

unknown_tasks = options[:tasks] - TASKS
abort "Unknown task(s): #{unknown_tasks.join(", ")}" unless unknown_tasks.empty?
abort "--iterations cannot be negative" if options[:iterations]&.negative?
abort "--warmups cannot be negative" if options[:warmups].negative?

manifest = load_manifest(options[:manifest])
selected_implementations =
  if options[:implementations].empty?
    manifest.keys
  else
    options[:implementations]
  end

abort "No implementations selected" if selected_implementations.empty?

implementation_specs = selected_implementations.map { |value| implementation_spec(value, manifest) }
implementation_specs.each { |spec| validate_implementation_spec!(spec) }
reference_spec = implementation_spec(options[:reference_implementation], manifest)
validate_implementation_spec!(reference_spec) if options[:verify]

files =
  if options[:files].empty?
    DEFAULT_FILES.to_a
  else
    options[:files]
  end

files = files.map do |path, iterations|
  [path, options[:iterations] || iterations || 1]
end

missing_files = files.map(&:first).reject { |path| File.file?(path) }
if missing_files.any?
  if options[:skip_missing]
    files.reject! { |path, _iterations| missing_files.include?(path) }
  else
    abort "Missing CSV file(s): #{missing_files.join(", ")}"
  end
end

abort "No files to benchmark" if files.empty?

repo_root = Dir.pwd
ensure_docker_image!(options[:docker_image], options[:dockerfile], options[:build])
FileUtils.mkdir_p(File.dirname(options[:output]))
options[:expected_output] ||= options[:output].sub(/\.jsonl\z/, ".expected.json")

expected_results = {}

if options[:verify]
  warn "Computing expected results with #{reference_spec["name"]}"

  files.each do |file, _iterations|
    options[:tasks].each do |task|
      envelope = run_docker_measurement(
        options[:docker_image],
        repo_root,
        reference_spec,
        file,
        task,
        options[:balance_threshold]
      )

      unless envelope["exit_status"]&.zero? && envelope.dig("result", "result")
        abort "Reference failed for #{file} #{task}: #{JSON.generate(envelope)}"
      end

      expected_results[[file, task]] = envelope.dig("result", "result")
    end
  end

  expected_payload = expected_results.each_with_object([]) do |((file, task), result), payload|
    payload << {
      "file" => file,
      "task" => task,
      "reference_implementation" => reference_spec["name"],
      "result" => result,
      "canonical_result" => canonical_result(task, result)
    }
  end
  File.write(options[:expected_output], JSON.pretty_generate(expected_payload))
  warn "Wrote #{options[:expected_output]}"
end

records = []
measured_runs = 0

File.open(options[:output], "w") do |output|
  implementation_specs.each do |implementation|
    files.each do |file, iterations|
      options[:tasks].each do |task|
        total_runs = options[:warmups] + iterations

        total_runs.times do |run_index|
          warmup = run_index < options[:warmups]
          next if !warmup && options[:limit_runs] && measured_runs >= options[:limit_runs]

          iteration = warmup ? run_index + 1 : run_index - options[:warmups] + 1
          warn "#{implementation["name"]} #{file} #{task} #{warmup ? "warmup" : "run"} #{iteration}/#{warmup ? options[:warmups] : iterations}"

          envelope = run_docker_measurement(
            options[:docker_image],
            repo_root,
            implementation,
            file,
            task,
            options[:balance_threshold]
          )

          actual_result = envelope.dig("result", "result")
          correctness =
            if options[:verify]
              correctness_for(task, actual_result, expected_results.fetch([file, task]))
            else
              { "correct" => nil }
            end

          record = envelope.merge(
            "implementation" => implementation["name"],
            "implementation_path" => implementation["script"],
            "implementation_gemfile" => implementation["gemfile"],
            "file" => file,
            "task" => task,
            "iteration" => iteration,
            "warmup" => warmup,
            "correct" => correctness["correct"]
          )
          record["correctness"] = correctness unless correctness["correct"] == true

          output.puts(JSON.generate(record))
          output.flush

          unless warmup
            records << record
            measured_runs += 1
          end
        end
      end
    end
  end
end

summary = summarize(records)
summary_path = options[:output].sub(/\.jsonl\z/, ".summary.json")
File.write(summary_path, JSON.pretty_generate(summary))

warn "Wrote #{options[:output]}"
warn "Wrote #{summary_path}"
