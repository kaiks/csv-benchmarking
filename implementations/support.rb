# frozen_string_literal: true

require "date"
require "json"
require "optparse"

module ImplementationSupport
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

  module_function

  def emit_checkpoint(name)
    $stderr.puts "#{CHECKPOINT_PREFIX} #{name}"
    $stderr.flush
  end

  def valid_date_of_birth?(value)
    match = /\A(\d{2})\/(\d{2})\/(\d{4})\z/.match(value.to_s)
    return false unless match

    Date.valid_date?(match[3].to_i, match[1].to_i, match[2].to_i)
  end

  def cents_from_decimal(value)
    return (value * 100).round if value.is_a?(Numeric)

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

  def parse_options!
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

    options[:threshold_cents] = (options[:balance_threshold] * 100).round
    options
  end

  def emit_result(implementation, task, result)
    puts JSON.generate(
      {
        "implementation" => implementation,
        "task" => task,
        "result" => result
      }
    )
  end
end
