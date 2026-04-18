#!/usr/bin/env ruby
# frozen_string_literal: true

require "cgi"
require "fileutils"
require "json"
require "optparse"

IMPLEMENTATION_ORDER = %w[
  ruby_csv_default
  smarter_csv
  polars
  polars_low_memory
  duckdb
].freeze

COLORS = {
  "ruby_csv_default" => "#0f766e",
  "smarter_csv" => "#2563eb",
  "polars" => "#dc2626",
  "polars_low_memory" => "#7c3aed",
  "duckdb" => "#ca8a04"
}.freeze

DISPLAY_NAMES = {
  "ruby_csv_default" => "Ruby CSV",
  "smarter_csv" => "SmarterCSV",
  "polars" => "Polars",
  "polars_low_memory" => "Polars low memory",
  "duckdb" => "DuckDB"
}.freeze

def esc(value)
  CGI.escapeHTML(value.to_s)
end

def file_slug(path)
  File.basename(path, ".csv").delete_prefix("people_")
end

def file_label(path)
  slug = file_slug(path)
  match = /\A(\d+)([km])\z/.match(slug)
  return slug unless match

  suffix = match[2] == "k" ? "K" : "M"
  "#{match[1]}#{suffix} rows"
end

def implementation_label(name)
  DISPLAY_NAMES.fetch(name, name)
end

def ordered(records)
  records.sort_by { |record| IMPLEMENTATION_ORDER.index(record["implementation"]) || IMPLEMENTATION_ORDER.length }
end

def metric(record, key)
  case key
  when :time
    record.dig("wall_time_seconds", "mean")
  when :peak_memory
    record.dig("peak_memory_mib", "max")
  when :post_read_memory
    record.dig("post_file_read_memory_mib", "max")
  else
    raise ArgumentError, "Unknown metric: #{key}"
  end
end

def nice_number(value)
  return "0" if value.to_f.zero?

  if value < 1
    format("%.3f", value)
  elsif value < 10
    format("%.2f", value)
  elsif value < 100
    format("%.1f", value)
  else
    value.round.to_s
  end
end

def max_metric(records, key)
  records.map { |record| metric(record, key).to_f }.max || 0
end

def axis_ticks(max_value, count = 4)
  return [0] if max_value.zero?

  (0..count).map { |index| max_value * index / count.to_f }
end

def grouped_bar_panel(records, metric_key, title, unit, x, y, width, height)
  max_value = max_metric(records, metric_key)
  max_value *= 1.12 unless max_value.zero?
  plot_left = x + 58
  plot_right = x + width - 18
  plot_top = y + 42
  plot_bottom = y + height - 82
  plot_width = plot_right - plot_left
  plot_height = plot_bottom - plot_top
  bar_gap = 14
  bar_width = (plot_width - (bar_gap * (records.length - 1))) / records.length.to_f

  svg = +""
  svg << %(<text x="#{x}" y="#{y + 20}" class="panel-title">#{esc(title)}</text>\n)

  axis_ticks(max_value).each do |tick|
    tick_y = plot_bottom - ((tick / max_value) * plot_height)
    svg << %(<line x1="#{plot_left}" y1="#{tick_y.round(2)}" x2="#{plot_right}" y2="#{tick_y.round(2)}" class="grid"/>\n)
    svg << %(<text x="#{plot_left - 10}" y="#{(tick_y + 4).round(2)}" class="axis-label" text-anchor="end">#{esc(nice_number(tick))}</text>\n)
  end

  svg << %(<line x1="#{plot_left}" y1="#{plot_bottom}" x2="#{plot_right}" y2="#{plot_bottom}" class="axis"/>\n)
  svg << %(<line x1="#{plot_left}" y1="#{plot_top}" x2="#{plot_left}" y2="#{plot_bottom}" class="axis"/>\n)

  records.each_with_index do |record, index|
    value = metric(record, metric_key).to_f
    bar_height = max_value.zero? ? 0 : (value / max_value) * plot_height
    bar_x = plot_left + index * (bar_width + bar_gap)
    bar_y = plot_bottom - bar_height
    color = COLORS.fetch(record["implementation"], "#525252")
    label_x = bar_x + (bar_width / 2.0)

    svg << %(<rect x="#{bar_x.round(2)}" y="#{bar_y.round(2)}" width="#{bar_width.round(2)}" height="#{bar_height.round(2)}" fill="#{color}"/>\n)
    svg << %(<text x="#{label_x.round(2)}" y="#{(bar_y - 7).round(2)}" class="value-label" text-anchor="middle">#{esc(nice_number(value))} #{esc(unit)}</text>\n)
    svg << %(<text x="#{label_x.round(2)}" y="#{plot_bottom + 18}" class="x-label" text-anchor="end" transform="rotate(-35 #{label_x.round(2)} #{plot_bottom + 18})">#{esc(implementation_label(record["implementation"]))}</text>\n)
  end

  svg
end

def svg_document(title, subtitle, body, width:, height:)
  <<~SVG
    <svg xmlns="http://www.w3.org/2000/svg" width="#{width}" height="#{height}" viewBox="0 0 #{width} #{height}" role="img" aria-label="#{esc(title)}">
      <style>
        .bg { fill: #fafafa; }
        .title { fill: #111827; font: 700 24px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        .subtitle { fill: #4b5563; font: 400 13px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        .panel-title { fill: #111827; font: 700 16px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        .axis { stroke: #374151; stroke-width: 1; }
        .grid { stroke: #e5e7eb; stroke-width: 1; }
        .axis-label, .x-label { fill: #4b5563; font: 400 11px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        .value-label { fill: #111827; font: 600 10px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        .note { fill: #4b5563; font: 400 12px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        .legend-text { fill: #111827; font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
      </style>
      <rect width="100%" height="100%" class="bg"/>
      <text x="32" y="38" class="title">#{esc(title)}</text>
      <text x="32" y="60" class="subtitle">#{esc(subtitle)}</text>
      #{body}
    </svg>
  SVG
end

def dataset_chart(records, file, output_dir)
  width = 1120
  height = 900
  file_records = ordered(records)
  title = "All Tasks Run: #{file_label(file)}"
  subtitle = "Each bar is one independent Docker-measured run of task=all; lower is better."
  body = +""
  body << grouped_bar_panel(file_records, :time, "Runtime", "s", 32, 92, 520, 380)
  body << grouped_bar_panel(file_records, :peak_memory, "Peak anonymous memory", "MiB", 584, 92, 504, 380)
  body << grouped_bar_panel(file_records, :post_read_memory, "Post-load anonymous memory", "MiB", 32, 512, 1056, 300)
  body << %(<text x="32" y="864" class="note">Correctness: #{file_records.sum { |r| r["correct_runs"].to_i }} correct runs, #{file_records.sum { |r| r["incorrect_runs"].to_i }} incorrect runs.</text>\n)
  body << %(<text x="32" y="884" class="note">Post-load memory is useful when a short parsing peak is acceptable and the loaded representation will be reused for multiple operations.</text>\n)

  path = File.join(output_dir, "all_tasks_#{file_slug(file)}.svg")
  File.write(path, svg_document(title, subtitle, body, width: width, height: height))
  path
end

def polyline(points)
  points.map { |x, y| "#{x.round(2)},#{y.round(2)}" }.join(" ")
end

def log_position(value, min_value, max_value, min_pos, max_pos)
  value = [value, min_value].max
  return min_pos if max_value <= min_value

  ratio = (Math.log10(value) - Math.log10(min_value)) / (Math.log10(max_value) - Math.log10(min_value))
  min_pos + ratio * (max_pos - min_pos)
end

def line_panel(records_by_file, metric_key, title, unit, x, y, width, height)
  files = records_by_file.keys.sort_by { |file| rows_from_slug(file_slug(file)) }
  implementations = ordered(records_by_file.values.flatten).map { |record| record["implementation"] }.uniq
  values = records_by_file.values.flatten.map { |record| metric(record, metric_key).to_f }.reject(&:zero?)
  min_value = [values.min || 1, 0.01].max
  max_value = values.max || 1
  plot_left = x + 58
  plot_right = x + width - 30
  plot_top = y + 42
  plot_bottom = y + height - 64
  file_x = files.each_with_index.to_h do |file, index|
    position = files.length == 1 ? (plot_left + plot_right) / 2.0 : plot_left + ((plot_right - plot_left) * index / (files.length - 1).to_f)
    [file, position]
  end

  svg = +""
  svg << %(<text x="#{x}" y="#{y + 20}" class="panel-title">#{esc(title)}</text>\n)

  axis_ticks(max_value).drop(1).each do |tick|
    tick = [tick, min_value].max
    tick_y = log_position(tick, min_value, max_value, plot_bottom, plot_top)
    svg << %(<line x1="#{plot_left}" y1="#{tick_y.round(2)}" x2="#{plot_right}" y2="#{tick_y.round(2)}" class="grid"/>\n)
    svg << %(<text x="#{plot_left - 10}" y="#{(tick_y + 4).round(2)}" class="axis-label" text-anchor="end">#{esc(nice_number(tick))}</text>\n)
  end

  svg << %(<line x1="#{plot_left}" y1="#{plot_bottom}" x2="#{plot_right}" y2="#{plot_bottom}" class="axis"/>\n)
  svg << %(<line x1="#{plot_left}" y1="#{plot_top}" x2="#{plot_left}" y2="#{plot_bottom}" class="axis"/>\n)

  files.each do |file|
    fx = file_x.fetch(file)
    svg << %(<text x="#{fx.round(2)}" y="#{plot_bottom + 24}" class="x-label" text-anchor="middle">#{esc(file_label(file))}</text>\n)
  end

  implementations.each do |implementation|
    points = files.filter_map do |file|
      record = records_by_file.fetch(file).find { |candidate| candidate["implementation"] == implementation }
      next unless record

      value = metric(record, metric_key).to_f
      [file_x.fetch(file), log_position(value, min_value, max_value, plot_bottom, plot_top)]
    end
    next if points.empty?

    color = COLORS.fetch(implementation, "#525252")
    svg << %(<polyline points="#{polyline(points)}" fill="none" stroke="#{color}" stroke-width="3"/>\n)
    points.each do |point_x, point_y|
      svg << %(<circle cx="#{point_x.round(2)}" cy="#{point_y.round(2)}" r="4" fill="#{color}"/>\n)
    end
  end

  legend_x = plot_left
  legend_y = plot_bottom + 46
  implementations.each_with_index do |implementation, index|
    lx = legend_x + (index * 162)
    color = COLORS.fetch(implementation, "#525252")
    svg << %(<rect x="#{lx}" y="#{legend_y - 10}" width="12" height="12" fill="#{color}"/>\n)
    svg << %(<text x="#{lx + 18}" y="#{legend_y}" class="legend-text">#{esc(implementation_label(implementation))}</text>\n)
  end

  svg << %(<text x="#{plot_right}" y="#{plot_top - 8}" class="note" text-anchor="end">#{esc(unit)}, log scale</text>\n)
  svg
end

def rows_from_slug(slug)
  match = /\A(\d+)([km])\z/.match(slug)
  return slug.to_i unless match

  multiplier = match[2] == "k" ? 1_000 : 1_000_000
  match[1].to_i * multiplier
end

def summary_chart(records_by_file, output_dir)
  width = 1160
  height = 980
  title = "All Tasks Summary"
  subtitle = "Runtime, peak memory, and post-load memory for task=all across generated CSV sizes; lower is better."
  body = +""
  body << line_panel(records_by_file, :time, "Runtime by file size", "seconds", 32, 92, 1096, 250)
  body << line_panel(records_by_file, :peak_memory, "Peak anonymous memory by file size", "MiB", 32, 382, 1096, 250)
  body << line_panel(records_by_file, :post_read_memory, "Post-load anonymous memory by file size", "MiB", 32, 672, 1096, 250)

  path = File.join(output_dir, "all_tasks_summary.svg")
  File.write(path, svg_document(title, subtitle, body, width: width, height: height))
  path
end

def post_load_chart(records_by_file, output_dir)
  files = records_by_file.keys.sort_by { |file| rows_from_slug(file_slug(file)) }
  width = 1120
  height = 92 + (files.length * 308)
  title = "Post-Load Memory"
  subtitle = "Anonymous memory at the post_file_read checkpoint for task=all; lower is better."
  body = +""

  files.each_with_index do |file, index|
    body << grouped_bar_panel(
      ordered(records_by_file.fetch(file)),
      :post_read_memory,
      file_label(file),
      "MiB",
      32,
      92 + (index * 308),
      1056,
      260
    )
  end

  body << %(<text x="32" y="#{height - 28}" class="note">This chart ignores transient parsing peaks and focuses on memory left after the file is loaded.</text>\n)

  path = File.join(output_dir, "post_load_memory.svg")
  File.write(path, svg_document(title, subtitle, body, width: width, height: height))
  path
end

def speedup_chart(records_by_file, output_dir)
  largest_file = records_by_file.keys.max_by { |file| rows_from_slug(file_slug(file)) }
  records = ordered(records_by_file.fetch(largest_file))
  baseline = records.find { |record| record["implementation"] == "ruby_csv_default" }
  return nil unless baseline

  baseline_time = metric(baseline, :time).to_f
  speedups = records.map do |record|
    record.merge("speedup" => baseline_time / metric(record, :time).to_f)
  end

  width = 880
  height = 470
  x = 54
  y = 96
  plot_left = x + 58
  plot_right = width - 42
  plot_top = y + 16
  plot_bottom = height - 92
  plot_width = plot_right - plot_left
  plot_height = plot_bottom - plot_top
  max_value = speedups.map { |record| record["speedup"] }.max * 1.15
  bar_gap = 16
  bar_width = (plot_width - (bar_gap * (speedups.length - 1))) / speedups.length.to_f
  body = +""

  body << %(<text x="32" y="86" class="panel-title">Speedup vs Ruby CSV on #{esc(file_label(largest_file))}</text>\n)
  axis_ticks(max_value).each do |tick|
    tick_y = plot_bottom - ((tick / max_value) * plot_height)
    body << %(<line x1="#{plot_left}" y1="#{tick_y.round(2)}" x2="#{plot_right}" y2="#{tick_y.round(2)}" class="grid"/>\n)
    body << %(<text x="#{plot_left - 10}" y="#{(tick_y + 4).round(2)}" class="axis-label" text-anchor="end">#{esc(nice_number(tick))}x</text>\n)
  end
  body << %(<line x1="#{plot_left}" y1="#{plot_bottom}" x2="#{plot_right}" y2="#{plot_bottom}" class="axis"/>\n)
  body << %(<line x1="#{plot_left}" y1="#{plot_top}" x2="#{plot_left}" y2="#{plot_bottom}" class="axis"/>\n)

  speedups.each_with_index do |record, index|
    value = record["speedup"]
    bar_height = (value / max_value) * plot_height
    bar_x = plot_left + index * (bar_width + bar_gap)
    bar_y = plot_bottom - bar_height
    label_x = bar_x + (bar_width / 2.0)
    color = COLORS.fetch(record["implementation"], "#525252")

    body << %(<rect x="#{bar_x.round(2)}" y="#{bar_y.round(2)}" width="#{bar_width.round(2)}" height="#{bar_height.round(2)}" fill="#{color}"/>\n)
    body << %(<text x="#{label_x.round(2)}" y="#{(bar_y - 8).round(2)}" class="value-label" text-anchor="middle">#{esc(format("%.1fx", value))}</text>\n)
    body << %(<text x="#{label_x.round(2)}" y="#{plot_bottom + 18}" class="x-label" text-anchor="end" transform="rotate(-35 #{label_x.round(2)} #{plot_bottom + 18})">#{esc(implementation_label(record["implementation"]))}</text>\n)
  end

  body << %(<text x="32" y="#{height - 30}" class="note">This is the notable outlier: vectorized engines are much faster on the largest available file, while Ruby parsers are competitive on tiny files.</text>\n)

  title = "Notable Result: Large File Speedup"
  subtitle = "Derived from independent task=all runs; Ruby CSV is the 1.0x baseline."
  path = File.join(output_dir, "surprise_large_file_speedup.svg")
  File.write(path, svg_document(title, subtitle, body, width: width, height: height))
  path
end

def build_index(paths, output_dir, source)
  links = paths.map do |path|
    name = File.basename(path)
    %(<li><a href="#{esc(name)}">#{esc(name)}</a></li>)
  end.join("\n")

  html = <<~HTML
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>CSV Benchmark Charts</title>
      <style>
        body { margin: 0; font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #111827; background: #fafafa; }
        main { max-width: 1160px; margin: 0 auto; padding: 32px 20px 48px; }
        h1 { font-size: 28px; margin: 0 0 8px; }
        p { color: #4b5563; line-height: 1.5; }
        ul { padding-left: 20px; }
        a { color: #2563eb; }
        section { margin-top: 32px; }
        img { width: 100%; height: auto; display: block; border: 1px solid #e5e7eb; border-radius: 8px; background: white; }
      </style>
    </head>
    <body>
      <main>
        <h1>CSV Benchmark Charts</h1>
        <p>Source summary: <code>#{esc(source)}</code>. Each chart uses stored benchmark results; no benchmark runs are executed while rendering charts.</p>
        <ul>
          #{links}
        </ul>
        #{paths.map { |path| %(<section><img src="#{esc(File.basename(path))}" alt="#{esc(File.basename(path))}"></section>) }.join("\n")}
      </main>
    </body>
    </html>
  HTML

  path = File.join(output_dir, "index.html")
  File.write(path, html)
  path
end

options = {
  summary: "benchmarks/results/all_implementations_all_task_validation.summary.json",
  output_dir: "benchmarks/charts",
  task: "all"
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options]"

  opts.on("--summary PATH", "Benchmark summary JSON") do |value|
    options[:summary] = value
  end

  opts.on("--output-dir DIR", "Chart output directory") do |value|
    options[:output_dir] = value
  end

  opts.on("--task TASK", "Task to chart, default: #{options[:task]}") do |value|
    options[:task] = value
  end
end

parser.parse!

records = JSON.parse(File.read(options[:summary])).select { |record| record["task"] == options[:task] }
abort "No records for task=#{options[:task]} in #{options[:summary]}" if records.empty?

records_by_file = records.group_by { |record| record["file"] }
FileUtils.mkdir_p(options[:output_dir])

paths = []
records_by_file.keys.sort_by { |file| rows_from_slug(file_slug(file)) }.each do |file|
  paths << dataset_chart(records_by_file.fetch(file), file, options[:output_dir])
end
paths << summary_chart(records_by_file, options[:output_dir])
paths << post_load_chart(records_by_file, options[:output_dir])
surprise_path = speedup_chart(records_by_file, options[:output_dir])
paths << surprise_path if surprise_path
index_path = build_index(paths, options[:output_dir], options[:summary])

puts JSON.pretty_generate(
  {
    "charts" => paths,
    "index" => index_path
  }
)
