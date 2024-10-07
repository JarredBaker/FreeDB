# FreeDB - Lightweight In-Memory Database

`FreeDB` is a lightweight, thread-safe, in-memory database system for Ruby that allows for the creation and management of tables with column definitions and data type enforcement. It also includes automatic persistence, enabling you to save and load the database state to and from a file.

## Features

- **Create Tables**: Define tables with column names and expected data types.
- **Data Type Enforcement**: Ensures that inserted records conform to the specified data types.
- **Querying**: Supports basic queries such as fetching rows, ordering by a column, and filtering rows based on conditions.
- **Thread Safety**: Uses `Mutex` to ensure thread-safe operations for creating tables and inserting records.
- **Persistence**: Save the database to a file and load it back with ease using Ruby's `Marshal` serialization.
- **Auto-Save and Auto-Load**: Automatically saves the database at program exit and auto-loads any saved data when the application starts.
