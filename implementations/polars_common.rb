# frozen_string_literal: true

require "polars-df"
require_relative "support"

module PolarsImplementation
  BALANCE_COLUMN = "account_balance"
  DOB_COLUMN = "date_of_birth"
  DOB_PATTERN = "^\\d{2}/\\d{2}/\\d{4}$"

  module_function

  def read_frame(file_path, low_memory:)
    Polars.read_csv(
      file_path,
      infer_schema_length: 0,
      low_memory: low_memory,
      rechunk: !low_memory
    )
  end

  def balance_expr
    Polars.col(BALANCE_COLUMN).cast(Polars::Float64)
  end

  def invalid_dob_expr
    Polars.col(DOB_COLUMN).str.contains(DOB_PATTERN).not_
  end

  def scalar(frame, column_name = nil)
    row = frame.row(0)
    return row.first unless column_name

    frame[column_name][0]
  end

  def total_cents(frame)
    (scalar(frame.select(balance_expr.sum), BALANCE_COLUMN) * 100).round
  end

  def high_balance_count(frame, threshold_cents)
    threshold = threshold_cents / 100.0
    scalar(frame.select((balance_expr > threshold).sum), BALANCE_COLUMN)
  end

  def invalid_indexes(frame)
    frame
      .with_row_index(name: "row_index")
      .filter(invalid_dob_expr)
      .select("row_index")["row_index"]
      .to_a
  end

  def read_file(frame)
    { "row_count" => frame.height }
  end

  def count_high_balances(frame, threshold_cents)
    {
      "row_count" => frame.height,
      "threshold_cents" => threshold_cents,
      "count" => high_balance_count(frame, threshold_cents)
    }
  end

  def count_duplicate_rows(frame)
    unique_rows = frame.n_unique

    {
      "row_count" => frame.height,
      "unique_rows" => unique_rows,
      "duplicate_rows" => frame.height - unique_rows
    }
  end

  def total_account_balance(frame)
    cents = total_cents(frame)

    {
      "row_count" => frame.height,
      "total_account_balance_cents" => cents,
      "total_account_balance" => ImplementationSupport.dollars_from_cents(cents)
    }
  end

  def count_invalid_dob_rows(frame)
    {
      "row_count" => frame.height,
      "invalid_dob_rows" => scalar(frame.select(invalid_dob_expr.sum), DOB_COLUMN)
    }
  end

  def invalid_dob_indexes(frame)
    indexes = invalid_indexes(frame)

    {
      "row_count" => frame.height,
      "invalid_dob_indexes" => ImplementationSupport.summarize_indexes(indexes)
    }
  end

  def all_metrics(frame, threshold_cents)
    unique_rows = frame.n_unique
    cents = total_cents(frame)
    indexes = invalid_indexes(frame)

    {
      "row_count" => frame.height,
      "threshold_cents" => threshold_cents,
      "count_high_balances" => high_balance_count(frame, threshold_cents),
      "unique_rows" => unique_rows,
      "duplicate_rows" => frame.height - unique_rows,
      "total_account_balance_cents" => cents,
      "total_account_balance" => ImplementationSupport.dollars_from_cents(cents),
      "invalid_dob_rows" => indexes.length,
      "invalid_dob_indexes" => ImplementationSupport.summarize_indexes(indexes)
    }
  end

  def run(implementation_name, low_memory:)
    options = ImplementationSupport.parse_options!
    frame = read_frame(options[:file], low_memory: low_memory)
    ImplementationSupport.emit_checkpoint("post_file_read")

    result =
      case options[:task]
      when "read"
        read_file(frame)
      when "count_high_balances"
        count_high_balances(frame, options[:threshold_cents])
      when "count_duplicate_rows"
        count_duplicate_rows(frame)
      when "total_account_balance"
        total_account_balance(frame)
      when "count_invalid_dob_rows"
        count_invalid_dob_rows(frame)
      when "invalid_dob_indexes"
        invalid_dob_indexes(frame)
      when "all"
        all_metrics(frame, options[:threshold_cents])
      end

    ImplementationSupport.emit_result(implementation_name, options[:task], result)
  end
end
