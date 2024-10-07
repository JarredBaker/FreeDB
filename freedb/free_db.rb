require_relative 'table'
require 'logger'

module DB
  class FreeDB
    DEFAULT_DB_FILE = 'free.db'.freeze

    def initialize
      @tables = {}
      @mutex = Mutex.new
      @logger = Logger.new($stdout)

      auto_load

      at_exit { auto_save }
    end

    def create_table(table_name, columns)
      Thread.new do
        @mutex.synchronize { safely_create_table(table_name.to_sym, columns) }
      end.join
    end

    def table(table_name)
      @tables[table_name.to_sym] || raise("Table '#{table_name}' does not exist.") if @tables.is_a?(Hash)
    end

    def save_to_file(filename = DEFAULT_DB_FILE)
      File.write(filename, Marshal.dump(@tables))
      @logger.info("Database saved to #{filename}")
    end

    def load_from_file(filename = DEFAULT_DB_FILE)
      raise "File '#{filename}' does not exist." unless File.exist?(filename)
      @tables = Marshal.load(File.read(filename))
      @logger.info("Database loaded from #{filename}")
    end

    def tables
      @tables.keys
    end

    def auto_save
      save_to_file(DEFAULT_DB_FILE)
    rescue => e
      @logger.error("Failed to save the database: #{e.message}")
    end

    private

    def auto_load
      @logger.info("No database found to load. Starting with an empty DB.") unless File.exist?(DEFAULT_DB_FILE)
      load_from_file(DEFAULT_DB_FILE)
      @logger.info("Auto-loaded database from #{DEFAULT_DB_FILE}")
    rescue => e
      @logger.error("Failed to load the database: #{e.message}")
    end

    def safely_create_table(table_name, columns)
      log_error("Table '#{table_name}' already exists.") and return if @tables.key?(table_name)
      @tables[table_name] = DB::Table.new(columns) unless @tables.nil?
      @logger.info("Table '#{table_name}' created with columns: #{columns.keys.join(', ')}")
    rescue => e
      log_error(e.message)
    end

    def log_error(message)
      @logger.error("An error occurred: #{message}")
    end
  end
end
