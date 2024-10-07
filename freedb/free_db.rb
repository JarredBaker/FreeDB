require_relative 'table'
require 'logger'

##
# The DB::FreeDB class provides an in-memory database system that allows for the creation and management
# of tables with customizable column definitions. Each table enforces data type integrity, allowing for
# well-structured data storage and querying.
#
# This class ensures thread safety through the use of a mutex, making it suitable for multi-threaded
# environments. Additionally, FreeDB includes automatic saving and loading functionality, allowing
# the database state to be persisted to disk using Ruby's `Marshal` serialization.
#
# Key features include:
# - Creation of tables with specified columns and data types.
# - Querying tables and retrieving rows.
# - Persistence of the database state to a file and loading from a file.
# - Thread-safe operations for table creation and record insertion.
# - Automatic save on program exit and auto-loading on initialization.
#
# @example Basic usage
#   db = DB::FreeDB.new
#   db.create_table('users', { name: String, age: Integer })
#   users_table = db.table('users')
#   db.save_to_file
#
# @see Mutex For thread-safety implementation.
# @see Marshal For serialization of the database state.
module DB
  class FreeDB
    DEFAULT_DB_FILE = 'free.db'.freeze

    # Initializes the FreeDB instance.
    #
    # Sets up the database by loading any previously saved data from the default
    # or specified file, and registers a hook to automatically save the database
    # state upon program exit.
    #
    # @example
    #   db = DB::FreeDB.new
    def initialize
      @tables = {}
      @mutex = Mutex.new
      @logger = Logger.new($stdout)

      auto_load

      at_exit { auto_save }
    end

    # Creates a new table with the given name and column definitions.
    #
    # The table name is symbolized, and the columns are defined by a hash where keys are column names
    # and values are the expected data types for each column.
    #
    # @param [String, Symbol] table_name The name of the table to create.
    # @param [Hash] columns The column definitions where keys are column names and values are data types.
    #
    # @raise [RuntimeError] If a table with the same name already exists.
    #
    # @example
    #   db.create_table('users', { name: String, age: Integer })
    def create_table(table_name, columns)
      Thread.new do
        @mutex.synchronize { safely_create_table(table_name.to_sym, columns) }
      end.join
    end

    # Retrieves the table with the specified name.
    #
    # @param [String, Symbol] table_name The name of the table to retrieve.
    # @return [DB::Table] The table object.
    # @raise [RuntimeError] If the table does not exist.
    #
    # @example
    #   users_table = db.table('users')
    def table(table_name)
      @tables[table_name.to_sym] || raise("Table '#{table_name}' does not exist.") if @tables.is_a?(Hash)
    end

    # Saves the current state of the database to a file.
    #
    # The data is serialized and saved to the specified file using `Marshal.dump`.
    #
    # @param [String] filename The file to save the database to. Defaults to 'free.db'.
    # @example
    #   db.save_to_file('backup.db')
    def save_to_file(filename = DEFAULT_DB_FILE)
      File.write(filename, Marshal.dump(@tables))
      @logger.info("Database saved to #{filename}")
    end

    # Loads the database state from a file.
    #
    # Reads the specified file and deserializes the data using `Marshal.load` to restore the database state.
    #
    # @param [String] filename The file to load the database from. Defaults to 'free.db'.
    # @raise [RuntimeError] If the file does not exist.
    #
    # @example
    #   db.load_from_file('backup.db')
    def load_from_file(filename = DEFAULT_DB_FILE)
      raise "File '#{filename}' does not exist." unless File.exist?(filename)
      @tables = Marshal.load(File.read(filename))
      @logger.info("Database loaded from #{filename}")
    end

    # Returns a list of all tables in the database.
    #
    # @return [Array<Symbol>] An array of table names (as symbols).
    # @example
    #   db.tables # => [:users, :orders]
    def tables
      @tables.keys
    end

    # Automatically saves the current state of the database to the default file.
    #
    # This method is called automatically at program exit.
    #
    # @example
    #   db.auto_save
    def auto_save
      save_to_file(DEFAULT_DB_FILE)
    rescue => e
      @logger.error("Failed to save the database: #{e.message}")
    end

    private

    # Automatically loads the database from the default file if it exists.
    #
    # If no database file is found, it starts with an empty database.
    def auto_load
      @logger.info("No database found to load. Starting with an empty DB.") unless File.exist?(DEFAULT_DB_FILE)
      load_from_file(DEFAULT_DB_FILE)
      @logger.info("Auto-loaded database from #{DEFAULT_DB_FILE}")
    rescue => e
      @logger.error("Failed to load the database: #{e.message}")
    end

    # Safely creates a table with the specified name and columns, ensuring thread safety.
    #
    # If a table with the same name already exists, an error is logged.
    #
    # @param [Symbol] table_name The name of the table to create.
    # @param [Hash] columns The column definitions where keys are column names and values are data types.
    def safely_create_table(table_name, columns)
      log_error("Table '#{table_name}' already exists.") and return if @tables.key?(table_name)
      @tables[table_name] = DB::Table.new(columns) unless @tables.nil?
      @logger.info("Table '#{table_name}' created with columns: #{columns.keys.join(', ')}")
    rescue => e
      log_error(e.message)
    end

    # Logs an error message.
    #
    # @param [String] message The error message to log.
    def log_error(message)
      @logger.error("An error occurred: #{message}")
    end
  end
end
