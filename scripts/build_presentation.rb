#!/usr/bin/env ruby
# frozen_string_literal: true

require "cgi"
require "fileutils"
require "json"
require "optparse"
require "rqrcode"

IMPLEMENTATIONS = [
  {
    name: "ruby_csv_default",
    label: "Ruby CSV",
    files: ["implementations/ruby_csv_default.rb"],
    note: "stdlib CSV.foreach baseline"
  },
  {
    name: "smarter_csv",
    label: "SmarterCSV",
    files: ["implementations/smarter_csv.rb"],
    note: "chunked rows from smarter_csv 1.16.3"
  },
  {
    name: "polars",
    label: "Polars",
    files: ["implementations/polars.rb", "implementations/polars_common.rb"],
    note: "polars-df 0.25.1 eager read"
  },
  {
    name: "polars_low_memory",
    label: "Polars low memory",
    files: ["implementations/polars_low_memory.rb", "implementations/polars_common.rb"],
    note: "same Polars code path with low_memory: true"
  },
  {
    name: "duckdb",
    label: "DuckDB",
    files: ["implementations/duckdb.rb"],
    note: "duckdb 1.5.2.0 Ruby binding"
  }
].freeze

DATA_FILES = [
  ["data/people_1k.csv", "1K rows"],
  ["data/people_10k.csv", "10K rows"],
  ["data/people_1m.csv", "1M rows"]
].freeze

CHARTS = {
  "1K all tasks" => "benchmarks/charts/all_tasks_1k.svg",
  "10K all tasks" => "benchmarks/charts/all_tasks_10k.svg",
  "1M all tasks" => "benchmarks/charts/all_tasks_1m.svg",
  "Summary" => "benchmarks/charts/all_tasks_summary.svg",
  "Post-load memory" => "benchmarks/charts/post_load_memory.svg",
  "Large-file speedup" => "benchmarks/charts/surprise_large_file_speedup.svg"
}.freeze

DISPLAY_NAMES = IMPLEMENTATIONS.to_h { |implementation| [implementation[:name], implementation[:label]] }.freeze

def esc(value)
  CGI.escapeHTML(value.to_s)
end

def source_loc(paths)
  paths.sum do |path|
    File.readlines(path).count do |line|
      stripped = line.strip
      !stripped.empty? && !stripped.start_with?("#")
    end
  end
end

def bytes(path)
  File.size?(path) || 0
end

def human_size(size)
  units = %w[B KiB MiB GiB]
  value = size.to_f
  unit = units.shift

  while value >= 1024 && units.any?
    value /= 1024.0
    unit = units.shift
  end

  value >= 10 ? "#{value.round} #{unit}" : "#{format("%.1f", value)} #{unit}"
end

def metric(record, path)
  path.reduce(record) { |value, key| value&.[](key) }
end

def format_number(value, digits = 2)
  return "" if value.nil?

  value = value.to_f
  if value >= 100
    value.round.to_s
  elsif value >= 10
    format("%.1f", value)
  else
    format("%.#{digits}f", value)
  end
end

def slide(content, classes: nil)
  class_attr = ["slide", classes].compact.join(" ")
  %(<section class="#{esc(class_attr)}">\n#{content}\n</section>\n)
end

def table(headers, rows)
  header_html = headers.map { |header| "<th>#{esc(header)}</th>" }.join
  row_html = rows.map do |row|
    "<tr>#{row.map { |cell| "<td>#{cell}</td>" }.join}</tr>"
  end.join("\n")

  <<~HTML
    <table>
      <thead><tr>#{header_html}</tr></thead>
      <tbody>
        #{row_html}
      </tbody>
    </table>
  HTML
end

def chart(path)
  abort "Missing chart: #{path}" unless File.file?(path)

  %(<div class="chart">#{File.read(path)}</div>)
end

def all_task_records(summary)
  JSON.parse(File.read(summary)).select { |record| record["task"] == "all" }
end

def records_for_file(records, file)
  records
    .select { |record| record["file"] == file }
    .sort_by { |record| IMPLEMENTATIONS.index { |implementation| implementation[:name] == record["implementation"] } || 100 }
end

def fastest(records)
  records.min_by { |record| metric(record, %w[wall_time_seconds mean]).to_f }
end

def lowest_post_load(records)
  records.min_by { |record| metric(record, %w[post_file_read_memory_mib max]).to_f }
end

def css
  <<~CSS
    @page {
      size: 13.333in 7.5in;
      margin: 0;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      background: #d4d4d8;
      color: #111827;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    .slide {
      width: 13.333in;
      height: 7.5in;
      page-break-after: always;
      break-after: page;
      background: #fafafa;
      padding: 0.52in 0.62in;
      position: relative;
      overflow: hidden;
    }

    .slide:last-child {
      page-break-after: auto;
      break-after: auto;
    }

    h1, h2, h3, p {
      margin: 0;
    }

    h1 {
      font-size: 48px;
      line-height: 1.02;
      letter-spacing: 0;
      max-width: 9.6in;
    }

    h2 {
      font-size: 34px;
      line-height: 1.1;
      margin-bottom: 0.28in;
      letter-spacing: 0;
    }

    h3 {
      font-size: 19px;
      margin-bottom: 0.08in;
    }

    p, li, td, th {
      font-size: 16px;
      line-height: 1.38;
    }

    .subtitle {
      margin-top: 0.22in;
      font-size: 22px;
      color: #4b5563;
      max-width: 8.8in;
    }

    .eyebrow {
      color: #0f766e;
      font-weight: 800;
      font-size: 15px;
      text-transform: uppercase;
      letter-spacing: 0;
      margin-bottom: 0.18in;
    }

    .grid-2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0.32in;
      align-items: start;
    }

    .grid-3 {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 0.22in;
    }

    .metric {
      border: 1px solid #d1d5db;
      border-radius: 8px;
      padding: 0.2in;
      min-height: 1.1in;
      background: #ffffff;
    }

    .metric strong {
      display: block;
      font-size: 30px;
      line-height: 1.05;
      margin-bottom: 0.08in;
    }

    .muted {
      color: #4b5563;
    }

    ul {
      margin: 0;
      padding-left: 0.24in;
    }

    li {
      margin-bottom: 0.1in;
    }

    table {
      border-collapse: collapse;
      width: 100%;
      background: #ffffff;
      border: 1px solid #d1d5db;
      border-radius: 8px;
      overflow: hidden;
    }

    th, td {
      border-bottom: 1px solid #e5e7eb;
      padding: 0.11in 0.14in;
      text-align: left;
      vertical-align: top;
    }

    th {
      background: #f3f4f6;
      font-size: 14px;
      text-transform: uppercase;
      color: #374151;
    }

    tr:last-child td {
      border-bottom: 0;
    }

    code, pre {
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
    }

    pre {
      margin: 0;
      white-space: pre-wrap;
      background: #111827;
      color: #f9fafb;
      border-radius: 8px;
      padding: 0.22in;
      font-size: 14px;
      line-height: 1.34;
    }

    .chart {
      width: 100%;
      height: 5.82in;
      display: flex;
      align-items: center;
      justify-content: center;
      background: #ffffff;
      border: 1px solid #d1d5db;
      border-radius: 8px;
      overflow: hidden;
      padding: 0.08in;
    }

    .chart svg {
      width: 100%;
      height: 100%;
      display: block;
    }

    .chart-short {
      height: 5.45in;
    }

    .footer {
      position: absolute;
      left: 0.62in;
      right: 0.62in;
      bottom: 0.26in;
      color: #6b7280;
      font-size: 12px;
      display: flex;
      justify-content: space-between;
    }

    .takeaway {
      font-size: 24px;
      line-height: 1.3;
      margin-top: 0.18in;
      max-width: 10.2in;
    }

    .qr-layout {
      display: grid;
      grid-template-columns: 4.6in 1fr;
      gap: 0.58in;
      align-items: center;
      margin-top: 0.32in;
    }

    .qr-box {
      width: 4.45in;
      height: 4.45in;
      background: #ffffff;
      border: 1px solid #d1d5db;
      border-radius: 8px;
      padding: 0.22in;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .qr-box svg {
      width: 100%;
      height: 100%;
      display: block;
    }

    .repo-url {
      font-size: 24px;
      font-weight: 700;
      color: #0f766e;
      overflow-wrap: anywhere;
      margin-top: 0.18in;
    }

    .small {
      font-size: 13px;
    }

    .snippet-note {
      color: #4b5563;
      margin-bottom: 0.18in;
      max-width: 10.8in;
    }

    .quick-start-code {
      font-size: 18px;
      line-height: 1.38;
      min-height: 4.5in;
    }

    .title-slide {
      display: flex;
      flex-direction: column;
      justify-content: center;
    }
  CSS
end

def minimal_dob_count_code
  <<~RUBY
    require "csv"
    require "date"

    invalid = 0

    CSV.foreach(file, headers: true).with_index do |row, index|
      dob = row["date_of_birth"].to_s
      match = /\\A(\\d{2})\\/(\\d{2})\\/(\\d{4})\\z/.match(dob)
      valid = match && Date.valid_date?(
        match[3].to_i, match[1].to_i, match[2].to_i
      )

      invalid += 1 unless valid
    end

    puts invalid
  RUBY
end

def quick_start_snippets
  [
    [
      "Ruby CSV",
      <<~RUBY
        require "csv"

        high_balance_rows = 0

        CSV.foreach("people.csv", headers: true) do |row|
          balance = row["account_balance"].to_f
          high_balance_rows += 1 if balance > 10_000
        end

        puts high_balance_rows
      RUBY
    ],
    [
      "SmarterCSV",
      <<~RUBY
        require "smarter_csv"

        csv_options = {
          chunk_size: 10_000, col_sep: ",", row_sep: "\\n",
          headers: { only: :account_balance },
          convert_values_to_numeric: { only: :account_balance },
          quote_escaping: :double_quotes,
          strip_whitespace: false, verbose: :quiet
        }

        high_balance_rows = 0

        SmarterCSV.process("people.csv", csv_options) do |rows|
          rows.each { |r| high_balance_rows += 1 if r[:account_balance] > 10_000 }
        end

        puts high_balance_rows
      RUBY
    ],
    [
      "Polars",
      <<~RUBY
        require "polars-df"

        df = Polars.read_csv(
          "people.csv",
          schema_overrides: { "account_balance" => Polars::Float64 }
        )

        result = df
          .select((Polars.col("account_balance") > 10_000).sum)
          .row(0)
          .first

        puts result
      RUBY
    ],
    [
      "Polars low memory",
      <<~RUBY
        require "polars-df"

        df = Polars.read_csv(
          "people.csv",
          low_memory: true,
          schema_overrides: { "account_balance" => Polars::Float64 }
        )

        result = df
          .select((Polars.col("account_balance") > 10_000).sum)
          .row(0)
          .first

        puts result
      RUBY
    ],
    [
      "DuckDB",
      <<~RUBY
        require "duckdb"

        db = DuckDB::Database.open(":memory:")
        con = db.connect

        result = con.query(<<~SQL).first.first
          SELECT count(*)
          FROM read_csv('people.csv', header = true, auto_detect = true)
          WHERE account_balance > 10000
        SQL

        puts result
      RUBY
    ]
  ]
end

def qr_svg(url)
  RQRCode::QRCode
    .new(url)
    .as_svg(
      color: "111827",
      fill: "ffffff",
      module_size: 8,
      shape_rendering: "crispEdges",
      standalone: true,
      use_path: true
    )
    .sub(/\A<\?xml[^>]+>\s*/, "")
    .sub("<svg ", %(<svg role="img" aria-label="QR code link to repository" ))
end

def render(summary, repo_url)
  records = all_task_records(summary)
  raise "No task=all records in #{summary}" if records.empty?

  data_rows = DATA_FILES.map do |path, label|
    [
      esc(label),
      esc(human_size(bytes(path))),
      esc("first_name, last_name, date_of_birth, account_balance"),
      esc("10% invalid DOB")
    ]
  end

  loc_rows = IMPLEMENTATIONS.map do |implementation|
    [
      esc(implementation[:label]),
      esc(source_loc(implementation[:files])),
      esc(implementation[:files].join(" + ")),
      esc(implementation[:note])
    ]
  end

  one_m_records = records_for_file(records, "data/people_1m.csv")
  fastest_one_m = fastest(one_m_records)
  lowest_mem_one_m = lowest_post_load(one_m_records)

  slides = []

  slides << slide(<<~HTML, classes: "title-slide")
    <p class="eyebrow">CSV processing benchmark</p>
    <h1>How fast and memory hungry are Ruby CSV options?</h1>
    <p class="subtitle">A reproducible benchmark comparing stdlib CSV, SmarterCSV, Polars, Polars low-memory, and DuckDB on generated person-account data.</p>
  HTML

  slides << slide(<<~HTML)
    <h2>Benchmark Setup</h2>
    <div class="grid-2">
      <div>
        <h3>Dataset</h3>
        <ul>
          <li>Rows generated with Faker.</li>
          <li>Columns: first name, last name, date of birth, account balance.</li>
          <li>Valid DOB format: <code>mm/dd/yyyy</code>.</li>
          <li>Invalid DOBs: roughly 10%, split between empty and <code>mm-dd-yy</code>.</li>
          <li>Balances range from -1000.00 to 1000000.00.</li>
        </ul>
      </div>
      <div>
        <h3>Measured Work</h3>
        <ul>
          <li>Read the file.</li>
          <li>Count balances above 10000.00.</li>
          <li>Count duplicate rows.</li>
          <li>Total all balances.</li>
          <li>Count invalid DOB rows.</li>
          <li>Collect invalid DOB row indexes.</li>
          <li>Run all metrics together in one task.</li>
        </ul>
      </div>
    </div>
  HTML

  slides << slide(<<~HTML)
    <h2>Files And Stored Results</h2>
    #{table(["File", "Size", "Columns", "Invalid DOB target"], data_rows)}
    <p class="muted" style="margin-top:0.22in">Charts in this deck use <code>#{esc(summary)}</code>. The stored data is a one-run validation summary, not the full repeated benchmark matrix.</p>
  HTML

  slides << slide(<<~HTML)
    <h2>Runner Model</h2>
    <div class="grid-3">
      <div class="metric">
        <strong>1 task</strong>
        <p>Each task runs as its own Docker-measured process.</p>
      </div>
      <div class="metric">
        <strong>1 bundle</strong>
        <p>Each implementation activates only its own Gemfile.</p>
      </div>
      <div class="metric">
        <strong>2 memory points</strong>
        <p>Peak anonymous memory and post-load anonymous memory.</p>
      </div>
    </div>
    <p class="takeaway">The <code>all</code> benchmark is also its own process. It is not a summary of the individual task runs.</p>
  HTML

  slides << slide(<<~HTML)
    <h2>A Minimal Task Implementation</h2>
    <p class="muted" style="margin-bottom:0.2in">This is the shape of the invalid DOB count task in the simplest Ruby CSV version.</p>
    <pre>#{esc(minimal_dob_count_code)}</pre>
  HTML

  slides << slide(<<~HTML)
    <h2>Implementation Size</h2>
    #{table(["Implementation", "LOC", "Files counted", "Notes"], loc_rows)}
    <p class="muted small" style="margin-top:0.18in">LOC excludes blank lines and full-line comments. Polars entries include the shared Polars implementation helper.</p>
  HTML

  quick_start_snippets.each do |label, code|
    slides << slide(<<~HTML)
      <h2>Quick Start: #{esc(label)}</h2>
      <p class="snippet-note">Dedicated minimal example, not benchmark code: read <code>people.csv</code> and count rows where <code>account_balance &gt; 10000</code>.</p>
      <pre class="quick-start-code">#{esc(code)}</pre>
    HTML
  end

  [["1K all tasks", "All Tasks: 1K Rows"], ["10K all tasks", "All Tasks: 10K Rows"], ["1M all tasks", "All Tasks: 1M Rows"]].each do |chart_name, title|
    slides << slide(<<~HTML)
      <h2>#{esc(title)}</h2>
      #{chart(CHARTS.fetch(chart_name))}
    HTML
  end

  slides << slide(<<~HTML)
    <h2>Post-Load Memory</h2>
    #{chart(CHARTS.fetch("Post-load memory"))}
  HTML

  slides << slide(<<~HTML)
    <h2>Summary Across File Sizes</h2>
    #{chart(CHARTS.fetch("Summary"))}
  HTML

  slides << slide(<<~HTML)
    <h2>Surprise: Large-File Speedup</h2>
    #{chart(CHARTS.fetch("Large-file speedup"))}
  HTML

  slides << slide(<<~HTML)
    <h2>What Stands Out</h2>
    <div class="grid-2">
      <div>
        <h3>Fastest 1M all-tasks run</h3>
        <p class="takeaway">#{esc(DISPLAY_NAMES.fetch(fastest_one_m["implementation"]))}: #{esc(format_number(metric(fastest_one_m, %w[wall_time_seconds mean])))} seconds.</p>
      </div>
      <div>
        <h3>Lowest 1M post-load memory</h3>
        <p class="takeaway">#{esc(DISPLAY_NAMES.fetch(lowest_mem_one_m["implementation"]))}: #{esc(format_number(metric(lowest_mem_one_m, %w[post_file_read_memory_mib max]), 1))} MiB.</p>
      </div>
    </div>
    <ul style="margin-top:0.34in">
      <li>Ruby CSV stays simple, but duplicate detection dominates memory on larger data.</li>
      <li>SmarterCSV improves the pure-Ruby path substantially for the combined workload.</li>
      <li>Polars and DuckDB pay more startup/setup cost on tiny files, then pull ahead on 1M rows.</li>
      <li>Post-load memory is the better metric when a transient parse peak is acceptable.</li>
    </ul>
  HTML

  slides << slide(<<~HTML)
    <p class="eyebrow">Contributions welcome</p>
    <h1>Try it, break it, add another parser.</h1>
    <div class="qr-layout">
      <div class="qr-box">#{qr_svg(repo_url)}</div>
      <div>
        <p class="takeaway">The benchmark runner is implementation-agnostic: add a script, add a manifest entry, and compare it against the same correctness checks.</p>
        <p class="repo-url">#{esc(repo_url)}</p>
        <ul style="margin-top:0.3in">
          <li>Add another Ruby CSV library.</li>
          <li>Improve the DuckDB or Polars implementations.</li>
          <li>Run the full repeated benchmark matrix on your machine.</li>
          <li>Send results, issues, and pull requests.</li>
        </ul>
      </div>
    </div>
  HTML

  slide_count = slides.length
  numbered = slides.each_with_index.map do |content, index|
    content.sub(
      "</section>",
      %(<div class="footer"><span>CSV Benchmarking</span><span>#{index + 1} / #{slide_count}</span></div>\n</section>)
    )
  end.join("\n")

  <<~HTML
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>CSV Processing Benchmarking</title>
      <style>#{css}</style>
    </head>
    <body>
      #{numbered}
    </body>
    </html>
  HTML
end

options = {
  summary: "benchmarks/results/all_implementations_all_task_validation.summary.json",
  output: "presentations/csv_benchmarking_talk.html",
  repo_url: "https://github.com/kaiks/csv-benchmarking"
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options]"

  opts.on("--summary PATH", "Benchmark summary JSON") do |value|
    options[:summary] = value
  end

  opts.on("--output PATH", "Output HTML path") do |value|
    options[:output] = value
  end

  opts.on("--repo-url URL", "Repository URL for the QR slide") do |value|
    options[:repo_url] = value
  end
end

parser.parse!

FileUtils.mkdir_p(File.dirname(options[:output]))
File.write(options[:output], render(options[:summary], options[:repo_url]))
puts options[:output]
