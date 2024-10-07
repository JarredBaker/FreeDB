require 'logger'
require_relative '../helpers/class_mapping'

module DB
  class Table
    def initialize(columns)
      @columns = columns.transform_keys(&:to_sym)
      @rows = []
      @mutex = Mutex.new
    end

    def insert(record)
      Thread.new do
        @mutex.synchronize { safely_insert(record.transform_keys(&:to_sym)) }
      end.join
    end

    def all
      @rows
    end

    def order_by(column, direction = :asc)
      sorted_rows = @rows.sort_by { |row| row[column.to_sym] }
      direction == :desc ? sorted_rows.reverse : sorted_rows
    end

    def limit(count)
      @rows.first(count)
    end

    def where(conditions)
      conditions = conditions.transform_keys(&:to_sym)
      @rows.select { |row| conditions.all? { |k, v| row[k] == v } }
    end

    private

    def safely_insert(record)
      raise "Column mismatch. Table expects: #{@columns.keys}" unless record.keys.sort == @columns.keys.sort
      enforce_data_types(record)
      @rows << record
    end

    def enforce_data_types(record)
      record.each do |col, value|
        expected_type = @columns[col]
        converted_value = convert_value_to_type(value, expected_type)

        unless converted_value.is_a?(expected_type.is_a?(Array) ? expected_type.first : expected_type)
          raise "Type mismatch for '#{col}'. Expected #{expected_type}, got #{converted_value.class}"
        end

        record[col] = converted_value
      end
    end

    def marshal_dump
      {
        columns: @columns,
        rows: @rows
      }
    end

    def marshal_load(data)
      @columns = data[:columns]
      @rows = data[:rows]
      @mutex = Mutex.new
    end
  end
end

