#!/usr/bin/env ruby
# frozen_string_literal: true

require "csv"
require "faker"
require "fileutils"
require "optparse"

PRESETS = {
  "1k" => 1_000,
  "10k" => 10_000,
  "1m" => 1_000_000,
  "10m" => 10_000_000,
  "100m" => 100_000_000
}.freeze

HEADERS = %w[first_name last_name date_of_birth account_balance].freeze
BALANCE_MIN = -1_000.0
BALANCE_MAX = 1_000_000.0

def progress_label(row_count)
  PRESETS.key(row_count) || row_count.to_s
end

def output_path_for(row_count, options)
  return options[:output] if options[:output]

  File.join(options[:output_dir], "people_#{progress_label(row_count)}.csv")
end

def validate_rate!(name, value)
  return if value.between?(0.0, 1.0)

  abort "#{name} must be between 0.0 and 1.0"
end

def write_csv(row_count, output_path, options)
  FileUtils.mkdir_p(File.dirname(output_path))

  invalid_total = (row_count * options[:invalid_rate]).round
  empty_invalid_remaining = (invalid_total * options[:empty_invalid_share]).round
  wrong_format_remaining = invalid_total - empty_invalid_remaining
  invalid_remaining = invalid_total
  rows_remaining = row_count
  progress_every = options[:progress_every]
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  CSV.open(output_path, "w", headers: HEADERS, write_headers: true, row_sep: "\n") do |csv|
    row_count.times do |index|
      birth_date = Faker::Date.birthday(min_age: options[:min_age], max_age: options[:max_age])
      date_of_birth =
        if invalid_remaining.positive? && options[:random].rand(rows_remaining) < invalid_remaining
          invalid_remaining -= 1

          if empty_invalid_remaining.positive? &&
             (wrong_format_remaining.zero? ||
              options[:random].rand(invalid_remaining + 1) < empty_invalid_remaining)
            empty_invalid_remaining -= 1
            ""
          else
            wrong_format_remaining -= 1
            birth_date.strftime("%m-%d-%y")
          end
        else
          birth_date.strftime("%m/%d/%Y")
        end

      account_balance = Faker::Number.within(range: BALANCE_MIN..BALANCE_MAX)

      csv << [
        Faker::Name.first_name,
        Faker::Name.last_name,
        date_of_birth,
        format("%.2f", account_balance)
      ]

      rows_remaining -= 1

      next unless progress_every.positive? && ((index + 1) % progress_every).zero?

      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
      rows_per_second = (index + 1) / elapsed
      warn format(
        "%<path>s: %<rows>d/%<total>d rows (%<rate>.0f rows/s)",
        path: output_path,
        rows: index + 1,
        total: row_count,
        rate: rows_per_second
      )
    end
  end

  warn "#{output_path}: wrote #{row_count} rows with #{invalid_total} invalid date_of_birth values"
end

options = {
  output_dir: "data",
  invalid_rate: 0.10,
  empty_invalid_share: 0.50,
  min_age: 18,
  max_age: 90,
  progress_every: 100_000,
  random: Random.new
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} (--rows N | --preset 1m|10m|100m | --all) [options]"

  opts.on("--rows N", Integer, "Generate a custom row count") do |value|
    options[:rows] = value
  end

  opts.on("--preset NAME", PRESETS.keys, "Generate one preset: #{PRESETS.keys.join(", ")}") do |value|
    options[:preset] = value
  end

  opts.on("--all", "Generate all presets: #{PRESETS.keys.join(", ")}") do
    options[:all] = true
  end

  opts.on("--output PATH", "Output path for single-file generation") do |value|
    options[:output] = value
  end

  opts.on("--output-dir DIR", "Output directory for preset generation") do |value|
    options[:output_dir] = value
  end

  opts.on("--invalid-rate RATE", Float, "Invalid DOB ratio, default: #{options[:invalid_rate]}") do |value|
    options[:invalid_rate] = value
  end

  opts.on(
    "--empty-invalid-share RATE",
    Float,
    "Share of invalid DOBs that are empty, default: #{options[:empty_invalid_share]}"
  ) do |value|
    options[:empty_invalid_share] = value
  end

  opts.on("--min-age YEARS", Integer, "Minimum age for generated birth dates") do |value|
    options[:min_age] = value
  end

  opts.on("--max-age YEARS", Integer, "Maximum age for generated birth dates") do |value|
    options[:max_age] = value
  end

  opts.on("--seed N", Integer, "Seed Ruby and Faker random generation") do |value|
    options[:seed] = value
  end

  opts.on("--progress-every N", Integer, "Progress interval in rows, use 0 to disable") do |value|
    options[:progress_every] = value
  end
end

parser.parse!

selected_modes = [options[:rows], options[:preset], options[:all]].count { |value| !value.nil? && value != false }
abort parser.to_s unless selected_modes == 1
abort "--rows must be positive" if options[:rows]&.<= 0
abort "--progress-every cannot be negative" if options[:progress_every].negative?
abort "--output can only be used with --rows or --preset" if options[:output] && options[:all]
abort "--min-age cannot be greater than --max-age" if options[:min_age] > options[:max_age]
validate_rate!("--invalid-rate", options[:invalid_rate])
validate_rate!("--empty-invalid-share", options[:empty_invalid_share])

if options[:seed]
  srand(options[:seed])
  options[:random] = Random.new(options[:seed])
  Faker::Config.random = Random.new(options[:seed])
end

jobs =
  if options[:all]
    PRESETS.values
  elsif options[:preset]
    [PRESETS.fetch(options[:preset])]
  else
    [options[:rows]]
  end

jobs.each do |row_count|
  write_csv(row_count, output_path_for(row_count, options), options)
end
