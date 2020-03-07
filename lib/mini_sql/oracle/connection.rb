# frozen_string_literal: true

module MiniSql
  module Oracle
    class Connection < MiniSql::Connection
      attr_reader :param_encoder, :raw_connection, :deserializer_cache

      # Initialize a new MiniSql::Oracle::Connection object
      #
      # @param raw_connection [OCI8] an active connection to Oracle
      # @param deserializer_cache [MiniSql::DeserializerCache] a cache of field names to deserializer, can be nil
      # @param param_encoder can be nil
      def initialize(raw_connection, args = nil)
        @raw_connection = raw_connection
        @param_encoder = (args && args[:param_encoder]) || InlineParamEncoder.new(self)
        @deserializer_cache = (args && args[:deserializer_cache]) || DeserializerCache.new
      end

      def query_single(sql, *params)
        raw_connection.select_one(param_encoder.encode(sql, *params))
      end

      def query_hash(sql, *params)
        cursor = raw_connection.parse(param_encoder.encode(sql, *params))
        cursor.exec
        r = []
        cursor.fetch_hash do |h|
          r << h
        end
        r
      ensure
        cursor.close if cursor
      end

      def query_array(sql, *params)
        r = []
        run(sql, params) do |a|
          r << a
        end
        r
      end

      def query(sql, *params)
        cursor = raw_connection.parse(param_encoder.encode(sql, *params))
        cursor.exec
        deserializer_cache.materialize(cursor)
      end

      def query_decorator(decorator, sql, *params)
        cursor = raw_connection.parse(param_encoder.encode(sql, *params))
        cursor.exec
        deserializer_cache.materialize(cursor, decorator)
      end

      def exec(sql, *params)
        run(sql, params)
      end

      private

      def run(sql, params)
        if params && params.length > 0
          sql = param_encoder.encode(sql, *params)
        end

        if block_given?
          raw_connection.exec(sql, &Proc.new)
        else
          raw_connection.exec(sql)
        end
      end
    end
  end
end
