#!/usr/bin/env ruby
# frozen_string_literal: true

require "csv"
require "date"
require "json"
require "optparse"

CHECKPOINT_PREFIX = "__CSV_BENCHMARK_CHECKPOINT__"
TASKS = %w[
  read
  count_high_balances
  count_duplicate_rows
  total_account_balance
  count_invalid_dob_rows
  invalid_dob_indexes
  all
].freeze

def emit_checkpoint(name)
  $stderr.puts "#{CHECKPOINT_PREFIX} #{name}"
  $stderr.flush
end

def valid_date_of_birth?(value)
  match = /\A(\d{2})\/(\d{2})\/(\d{4})\z/.match(value.to_s)
  return false unless match

  month = match[1].to_i
  day = match[2].to_i
  year = match[3].to_i

  Date.valid_date?(year, month, day)
end

def cents_from_decimal_string(value)
  raw = value.to_s
  negative = raw.start_with?("-")
  raw = raw.delete_prefix("-")

  whole, fractional = raw.split(".", 2)
  cents = whole.to_i * 100
  cents += fractional.to_s.ljust(2, "0")[0, 2].to_i
  negative ? -cents : cents
end

def dollars_from_cents(cents)
  cents / 100.0
end

def summarize_indexes(indexes)
  checksum = 0
  indexes.each_with_index do |index, position|
    checksum = (checksum + ((position + 1) * (index + 1))) & 0xffff_ffff_ffff_ffff
  end

  {
    "index_base" => 0,
    "materialized" => true,
    "count" => indexes.length,
    "first" => indexes.first(10),
    "last" => indexes.last(10),
    "checksum" => checksum
  }
end

def each_row(file_path)
  row_index = 0

  CSV.foreach(file_path, headers: true) do |row|
    yield row, row_index
    row_index += 1
  end

  row_index
end

def read_file(file_path)
  row_count = 0
  each_row(file_path) { row_count += 1 }
  emit_checkpoint("post_file_read")

  { "row_count" => row_count }
end

def count_high_balances(file_path, threshold_cents)
  count = 0
  row_count = each_row(file_path) do |row|
    count += 1 if cents_from_decimal_string(row["account_balance"]) > threshold_cents
  end
  emit_checkpoint("post_file_read")

  { "row_count" => row_count, "threshold_cents" => threshold_cents, "count" => count }
end

def count_duplicate_rows(file_path)
  seen = {}
  duplicate_count = 0

  row_count = each_row(file_path) do |row|
    key = row.fields

    if seen.key?(key)
      duplicate_count += 1
    else
      seen[key] = true
    end
  end
  emit_checkpoint("post_file_read")

  {
    "row_count" => row_count,
    "unique_rows" => seen.length,
    "duplicate_rows" => duplicate_count
  }
end

def total_account_balance(file_path)
  total_cents = 0
  row_count = each_row(file_path) do |row|
    total_cents += cents_from_decimal_string(row["account_balance"])
  end
  emit_checkpoint("post_file_read")

  {
    "row_count" => row_count,
    "total_account_balance_cents" => total_cents,
    "total_account_balance" => dollars_from_cents(total_cents)
  }
end

def count_invalid_dob_rows(file_path)
  count = 0
  row_count = each_row(file_path) do |row|
    count += 1 unless valid_date_of_birth?(row["date_of_birth"])
  end
  emit_checkpoint("post_file_read")

  { "row_count" => row_count, "invalid_dob_rows" => count }
end

def invalid_dob_indexes(file_path)
  indexes = []
  row_count = each_row(file_path) do |row, row_index|
    indexes << row_index unless valid_date_of_birth?(row["date_of_birth"])
  end
  emit_checkpoint("post_file_read")

  {
    "row_count" => row_count,
    "invalid_dob_indexes" => summarize_indexes(indexes)
  }
end

def all_metrics(file_path, threshold_cents)
  seen = {}
  duplicate_count = 0
  high_balance_count = 0
  total_cents = 0
  invalid_indexes = []

  row_count = each_row(file_path) do |row, row_index|
    key = row.fields

    if seen.key?(key)
      duplicate_count += 1
    else
      seen[key] = true
    end

    balance_cents = cents_from_decimal_string(row["account_balance"])
    high_balance_count += 1 if balance_cents > threshold_cents
    total_cents += balance_cents

    invalid_indexes << row_index unless valid_date_of_birth?(row["date_of_birth"])
  end
  emit_checkpoint("post_file_read")

  {
    "row_count" => row_count,
    "threshold_cents" => threshold_cents,
    "count_high_balances" => high_balance_count,
    "unique_rows" => seen.length,
    "duplicate_rows" => duplicate_count,
    "total_account_balance_cents" => total_cents,
    "total_account_balance" => dollars_from_cents(total_cents),
    "invalid_dob_rows" => invalid_indexes.length,
    "invalid_dob_indexes" => summarize_indexes(invalid_indexes)
  }
end

options = {
  balance_threshold: 10_000.0
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} --file PATH --task TASK [options]"

  opts.on("--file PATH", "CSV file path") do |value|
    options[:file] = value
  end

  opts.on("--task TASK", TASKS, "Task: #{TASKS.join(", ")}") do |value|
    options[:task] = value
  end

  opts.on("--balance-threshold N", Float, "Default: #{options[:balance_threshold]}") do |value|
    options[:balance_threshold] = value
  end
end

parser.parse!

abort parser.to_s unless options[:file] && options[:task]
abort "File not found: #{options[:file]}" unless File.file?(options[:file])

threshold_cents = (options[:balance_threshold] * 100).round

result =
  case options[:task]
  when "read"
    read_file(options[:file])
  when "count_high_balances"
    count_high_balances(options[:file], threshold_cents)
  when "count_duplicate_rows"
    count_duplicate_rows(options[:file])
  when "total_account_balance"
    total_account_balance(options[:file])
  when "count_invalid_dob_rows"
    count_invalid_dob_rows(options[:file])
  when "invalid_dob_indexes"
    invalid_dob_indexes(options[:file])
  when "all"
    all_metrics(options[:file], threshold_cents)
  end

puts JSON.generate(
  {
    "implementation" => "ruby_csv_default",
    "task" => options[:task],
    "result" => result
  }
)
