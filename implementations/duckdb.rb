#!/usr/bin/env ruby
# frozen_string_literal: true

require "duckdb"
require_relative "support"

IMPLEMENTATION = "duckdb"
DOB_PATTERN = "^\\d{2}/\\d{2}/\\d{4}$"

def sql_string(value)
  "'#{value.to_s.gsub("'", "''")}'"
end

def numeric(value)
  return value if value.is_a?(Numeric)

  value.to_s.include?(".") ? value.to_f : value.to_i
end

def result_rows(result)
  rows = []
  result.each { |row| rows << row }
  rows
end

def scalar_from_row(row)
  if row.is_a?(Hash)
    row.values.first
  elsif row.respond_to?(:fields)
    row.fields.first
  else
    row[0]
  end
end

class DuckDBBenchmark
  def initialize(file_path)
    @database = DuckDB::Database.open(":memory:")
    @connection = @database.connect
    escaped_path = sql_string(file_path)

    @connection.query(<<~SQL)
      CREATE TEMP TABLE people AS
      SELECT
        row_number() OVER () - 1 AS row_index,
        first_name,
        last_name,
        date_of_birth,
        account_balance
      FROM read_csv(
        #{escaped_path},
        header = true,
        all_varchar = true
      )
    SQL
  end

  def query(sql)
    result_rows(@connection.query(sql))
  end

  def scalar(sql)
    scalar_from_row(query(sql).first)
  end

  def row_count
    numeric(scalar("SELECT count(*) FROM people"))
  end

  def high_balance_count(threshold_cents)
    threshold = format("%.2f", threshold_cents / 100.0)
    numeric(
      scalar(
        "SELECT count(*) FROM people WHERE CAST(account_balance AS DECIMAL(18,2)) > CAST(#{sql_string(threshold)} AS DECIMAL(18,2))"
      )
    )
  end

  def unique_rows
    numeric(
      scalar(<<~SQL)
        SELECT count(*)
        FROM (
          SELECT DISTINCT first_name, last_name, date_of_birth, account_balance
          FROM people
        )
      SQL
    )
  end

  def total_cents
    numeric(
      scalar(
        "SELECT CAST(sum(CAST(CAST(account_balance AS DECIMAL(18,2)) * 100 AS BIGINT)) AS BIGINT) FROM people"
      )
    )
  end

  def invalid_count
    numeric(
      scalar(
        "SELECT count(*) FROM people WHERE NOT regexp_full_match(coalesce(date_of_birth, ''), #{sql_string(DOB_PATTERN)})"
      )
    )
  end

  def invalid_indexes
    query(
      "SELECT row_index FROM people WHERE NOT regexp_full_match(coalesce(date_of_birth, ''), #{sql_string(DOB_PATTERN)}) ORDER BY row_index"
    ).map { |row| numeric(scalar_from_row(row)) }
  end
end

options = ImplementationSupport.parse_options!
benchmark = DuckDBBenchmark.new(options[:file])
ImplementationSupport.emit_checkpoint("post_file_read")
row_count = benchmark.row_count

result =
  case options[:task]
  when "read"
    { "row_count" => row_count }
  when "count_high_balances"
    {
      "row_count" => row_count,
      "threshold_cents" => options[:threshold_cents],
      "count" => benchmark.high_balance_count(options[:threshold_cents])
    }
  when "count_duplicate_rows"
    unique_rows = benchmark.unique_rows

    {
      "row_count" => row_count,
      "unique_rows" => unique_rows,
      "duplicate_rows" => row_count - unique_rows
    }
  when "total_account_balance"
    total_cents = benchmark.total_cents

    {
      "row_count" => row_count,
      "total_account_balance_cents" => total_cents,
      "total_account_balance" => ImplementationSupport.dollars_from_cents(total_cents)
    }
  when "count_invalid_dob_rows"
    { "row_count" => row_count, "invalid_dob_rows" => benchmark.invalid_count }
  when "invalid_dob_indexes"
    indexes = benchmark.invalid_indexes

    {
      "row_count" => row_count,
      "invalid_dob_indexes" => ImplementationSupport.summarize_indexes(indexes)
    }
  when "all"
    unique_rows = benchmark.unique_rows
    total_cents = benchmark.total_cents
    indexes = benchmark.invalid_indexes

    {
      "row_count" => row_count,
      "threshold_cents" => options[:threshold_cents],
      "count_high_balances" => benchmark.high_balance_count(options[:threshold_cents]),
      "unique_rows" => unique_rows,
      "duplicate_rows" => row_count - unique_rows,
      "total_account_balance_cents" => total_cents,
      "total_account_balance" => ImplementationSupport.dollars_from_cents(total_cents),
      "invalid_dob_rows" => indexes.length,
      "invalid_dob_indexes" => ImplementationSupport.summarize_indexes(indexes)
    }
  end

ImplementationSupport.emit_result(IMPLEMENTATION, options[:task], result)
