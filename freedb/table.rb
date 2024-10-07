require 'logger'
require_relative '../helpers/class_mapping'

##
# The DB::Table class provides an in-memory table-like data structure that supports
# basic operations such as inserting records, querying rows, sorting, and limiting results.
# It ensures thread safety by using a mutex for concurrent access.
#
# This class allows you to define columns with their respective data types and
# ensures type enforcement during record insertion. It supports filtering rows
# based on specific conditions and ordering them by column values.
#
# Additionally, it supports serialization and deserialization via marshalling,
# making it suitable for persistence if needed.
#
# ## Key Features:
# - Thread-safe insertions using a mutex.
# - Supports querying with `where` conditions.
# - Can order rows by a specific column in ascending or descending order.
# - Limits the number of results with the `limit` method.
# - Ensures data type consistency based on column definitions.
#
# @example Creating a table and inserting a record
#   table = DB::Table.new({ name: String, age: Integer })
#   table.insert({ name: "John", age: 30 })
#   table.all # => [{ name: "John", age: 30 }]
#
# @see Mutex For thread safety implementation.
module DB
  class Table

    # Initializes a new Table object.
    #
    # @param [Hash] columns A hash mapping column names to their expected data types.
    #   Example: `{ name: String, age: Integer }`
    #
    # @example Initialize a new table with specific columns
    #   table = DB::Table.new({ name: String, age: Integer })
    def initialize(columns)
      @columns = columns.transform_keys(&:to_sym)
      @rows = []
      @mutex = Mutex.new
    end

    # Inserts a record into the table in a thread-safe manner.
    #
    # The record's keys must match the table's columns and will be inserted after
    # ensuring the correct data types are enforced.
    #
    # @param [Hash] record A hash representing a row to be inserted, with column names as keys.
    #   Example: `{ name: "John", age: 30 }`
    #
    # @raise [RuntimeError] If the record's columns do not match the table's expected columns.
    #
    # @example Insert a new record into the table
    #   table.insert({ name: "John", age: 30 })
    def insert(record)
      Thread.new do
        @mutex.synchronize { safely_insert(record.transform_keys(&:to_sym)) }
      end.join
    end

    # Returns all rows in the table.
    #
    # @return [Array<Hash>] An array of all records in the table.
    #
    # @example Get all rows in the table
    #   table.all
    def all
      @rows
    end

    # Orders the rows by a specified column in ascending or descending order.
    #
    # @param [Symbol, String] column The column to order by.
    # @param [Symbol] direction The direction to order by, either :asc (default) or :desc.
    # @return [Array<Hash>] The rows ordered by the specified column and direction.
    #
    # @example Order rows by the `age` column in descending order
    #   table.order_by(:age, :desc)
    def order_by(column, direction = :asc)
      sorted_rows = @rows.sort_by { |row| row[column.to_sym] }
      direction == :desc ? sorted_rows.reverse : sorted_rows
    end

    # Limits the number of rows returned from the table.
    #
    # @param [Integer] count The maximum number of rows to return.
    # @return [Array<Hash>] The first `count` rows from the table.
    #
    # @example Limit the number of rows to 5
    #   table.limit(5)
    def limit(count)
      @rows.first(count)
    end

    # Filters rows in the table based on specified conditions.
    #
    # @param [Hash] conditions A hash of conditions to filter rows by. The keys are column names and the values are the expected values in those columns.
    # @return [Array<Hash>] Rows that match the conditions.
    #
    # @example Find rows where the name is 'John'
    #   table.where({ name: "John" })
    def where(conditions)
      conditions = conditions.transform_keys(&:to_sym)
      @rows.select { |row| conditions.all? { |k, v| row[k] == v } }
    end

    private

    # Inserts a record safely, checking that the record's keys match the table's columns
    # and enforcing data types.
    #
    # @param [Hash] record The record to insert.
    # @raise [RuntimeError] If the record's keys do not match the table's columns.
    def safely_insert(record)
      raise "Column mismatch. Table expects: #{@columns.keys}" unless record.keys.sort == @columns.keys.sort
      enforce_data_types(record)
      @rows << record
    end

    # Enforces the correct data types for a record.
    #
    # This method converts values to their expected types based on the table's column definitions.
    #
    # @param [Hash] record The record whose data types will be enforced.
    # @raise [RuntimeError] If the type of a value does not match the expected column type.
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

    # Serializes the table for marshalling (storing object state).
    #
    # @return [Hash] A hash containing the columns and rows of the table.
    def marshal_dump
      {
        columns: @columns,
        rows: @rows
      }
    end

    # Loads the table's state from a marshalled hash.
    #
    # @param [Hash] data The data to load into the table (columns and rows).
    def marshal_load(data)
      @columns = data[:columns]
      @rows = data[:rows]
      @mutex = Mutex.new
    end
  end
end

