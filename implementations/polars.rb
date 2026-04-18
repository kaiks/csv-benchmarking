#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "polars_common"

PolarsImplementation.run("polars", low_memory: false)
