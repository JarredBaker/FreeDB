require 'date'

# Converts a string representing a type to the corresponding Ruby class.
#
# This method maps common type strings such as 'string', 'integer', 'date', etc.
# to their corresponding Ruby class (e.g., 'string' maps to `String` and 'date' maps to `Date`).
#
# If the provided string does not match any known type, an error is raised.
#
# @param [String] type_str the string representing the type to be converted
# @return [Class] the corresponding Ruby class for the provided type string
# @raise [RuntimeError] if the type string is unsupported
#
# @example
#   string_to_class('string')   # => String
#   string_to_class('integer')  # => Integer
#   string_to_class('date')     # => Date
def string_to_class(type_str)
  type_mapping = {
    'string' => String,
    'integer' => Integer,
    'float' => Float,
    'boolean' => [TrueClass, FalseClass],
    'symbol' => Symbol,
    'array' => Array,
    'hash' => Hash,
    'date' => Date,
    'time' => Time,
    'datetime' => DateTime,
    'nil' => NilClass,
    'rational' => Rational,
    'complex' => Complex
  }.freeze

  type_mapping[type_str.downcase] || raise("Unsupported type: #{type_str}")
end

# Converts a value to the specified type.
#
# This method attempts to convert a given value to the specified type.
# If the value is already of the correct type, it is returned as-is.
# The supported conversions include `Integer`, `Float`, `String`, `Symbol`, `Date`, `Time`, and `DateTime`.
# For booleans, the value must be 'true' or 'false' (case-insensitive).
#
# @param [Object] value the value to be converted
# @param [Class, Array<Class>] expected_type the target class or array of classes (for boolean handling)
# @return [Object] the value converted to the specified type
# @raise [RuntimeError] if the conversion is unsupported or if a boolean value is invalid
#
# @example
#   convert_value_to_type('123', Integer)         # => 123
#   convert_value_to_type('true', [TrueClass, FalseClass])  # => true
#   convert_value_to_type('2024-01-01', Date)     # => #<Date: 2024-01-01 ...>
#   convert_value_to_type('abc', String)          # => "abc"
def convert_value_to_type(value, expected_type)

  return value if value.is_a?(expected_type.is_a?(Array) ? expected_type.first : expected_type)

  if expected_type.is_a?(Array)
    if expected_type.include?(TrueClass) || expected_type.include?(FalseClass)
      return true if value.downcase == 'true'
      return false if value.downcase == 'false'
      raise "Invalid boolean value: #{value}"
    end
  end

  conversion_mapping = {
    integer: -> (value) {  Integer(value) },
    float: -> (value) { Float(value) },
    string: -> (value) { value.to_s },
    symbol: -> (value) { value.to_sym },
    date: -> (value) { Date.parse(value) },
    time: -> (value) { Time.parse(value) },
    datetime: -> (value) { DateTime.parse(value) }
  }

  conversion_mapping.find { |k, _| k == expected_type.downcase.to_sym }.then { |val| val[1].call(value) }  || raise("Unsupported conversion for type: #{expected_type}")
end
