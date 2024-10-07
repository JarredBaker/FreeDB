require 'date'

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
