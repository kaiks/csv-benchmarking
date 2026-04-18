#!/usr/bin/env ruby
# frozen_string_literal: true

require "smarter_csv"
require_relative "support"

IMPLEMENTATION = "smarter_csv"
CHUNK_SIZE = 10_000

def process_rows(file_path)
  row_index = 0

  SmarterCSV.process(file_path, chunk_size: CHUNK_SIZE) do |chunk|
    chunk.each do |row|
      yield row, row_index
      row_index += 1
    end
  end

  row_index
end

def row_key(row)
  [
    row[:first_name],
    row[:last_name],
    row[:date_of_birth],
    row[:account_balance]
  ]
end

def read_file(file_path)
  row_count = process_rows(file_path) {}
  ImplementationSupport.emit_checkpoint("post_file_read")

  { "row_count" => row_count }
end

def count_high_balances(file_path, threshold_cents)
  count = 0
  row_count = process_rows(file_path) do |row|
    count += 1 if ImplementationSupport.cents_from_decimal(row[:account_balance]) > threshold_cents
  end
  ImplementationSupport.emit_checkpoint("post_file_read")

  { "row_count" => row_count, "threshold_cents" => threshold_cents, "count" => count }
end

def count_duplicate_rows(file_path)
  seen = {}
  duplicate_count = 0

  row_count = process_rows(file_path) do |row|
    key = row_key(row)

    if seen.key?(key)
      duplicate_count += 1
    else
      seen[key] = true
    end
  end
  ImplementationSupport.emit_checkpoint("post_file_read")

  {
    "row_count" => row_count,
    "unique_rows" => seen.length,
    "duplicate_rows" => duplicate_count
  }
end

def total_account_balance(file_path)
  total_cents = 0
  row_count = process_rows(file_path) do |row|
    total_cents += ImplementationSupport.cents_from_decimal(row[:account_balance])
  end
  ImplementationSupport.emit_checkpoint("post_file_read")

  {
    "row_count" => row_count,
    "total_account_balance_cents" => total_cents,
    "total_account_balance" => ImplementationSupport.dollars_from_cents(total_cents)
  }
end

def count_invalid_dob_rows(file_path)
  count = 0
  row_count = process_rows(file_path) do |row|
    count += 1 unless ImplementationSupport.valid_date_of_birth?(row[:date_of_birth])
  end
  ImplementationSupport.emit_checkpoint("post_file_read")

  { "row_count" => row_count, "invalid_dob_rows" => count }
end

def invalid_dob_indexes(file_path)
  indexes = []
  row_count = process_rows(file_path) do |row, row_index|
    indexes << row_index unless ImplementationSupport.valid_date_of_birth?(row[:date_of_birth])
  end
  ImplementationSupport.emit_checkpoint("post_file_read")

  {
    "row_count" => row_count,
    "invalid_dob_indexes" => ImplementationSupport.summarize_indexes(indexes)
  }
end

def all_metrics(file_path, threshold_cents)
  seen = {}
  duplicate_count = 0
  high_balance_count = 0
  total_cents = 0
  invalid_indexes = []

  row_count = process_rows(file_path) do |row, row_index|
    key = row_key(row)

    if seen.key?(key)
      duplicate_count += 1
    else
      seen[key] = true
    end

    balance_cents = ImplementationSupport.cents_from_decimal(row[:account_balance])
    high_balance_count += 1 if balance_cents > threshold_cents
    total_cents += balance_cents

    invalid_indexes << row_index unless ImplementationSupport.valid_date_of_birth?(row[:date_of_birth])
  end
  ImplementationSupport.emit_checkpoint("post_file_read")

  {
    "row_count" => row_count,
    "threshold_cents" => threshold_cents,
    "count_high_balances" => high_balance_count,
    "unique_rows" => seen.length,
    "duplicate_rows" => duplicate_count,
    "total_account_balance_cents" => total_cents,
    "total_account_balance" => ImplementationSupport.dollars_from_cents(total_cents),
    "invalid_dob_rows" => invalid_indexes.length,
    "invalid_dob_indexes" => ImplementationSupport.summarize_indexes(invalid_indexes)
  }
end

options = ImplementationSupport.parse_options!

result =
  case options[:task]
  when "read"
    read_file(options[:file])
  when "count_high_balances"
    count_high_balances(options[:file], options[:threshold_cents])
  when "count_duplicate_rows"
    count_duplicate_rows(options[:file])
  when "total_account_balance"
    total_account_balance(options[:file])
  when "count_invalid_dob_rows"
    count_invalid_dob_rows(options[:file])
  when "invalid_dob_indexes"
    invalid_dob_indexes(options[:file])
  when "all"
    all_metrics(options[:file], options[:threshold_cents])
  end

ImplementationSupport.emit_result(IMPLEMENTATION, options[:task], result)
