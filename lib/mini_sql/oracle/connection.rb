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
        run(sql, params) do |cursor|
          cursor.fetch
        end
      end

      def query_hash(sql, *params)
        run(sql, params) do |cursor|
          r = []
          while h = cursor.fetch_hash
            r << h
          end
          r
        end
      end

      def query_array(sql, *params)
        run(sql, params) do |cursor|
          r = []
          while a = cursor.fetch
            r << a
          end
          r
        end
      end

      def query(sql, *params)
        run(sql, params) do |cursor|
          deserializer_cache.materialize(cursor)
        end
      end

      def query_decorator(decorator, sql, *params)
        run(sql, params) do |cursor|
          deserializer_cache.materialize(cursor, decorator)
        end
      end

      def exec(sql, *params)
        run(sql, params)
      end

      def escape_string(str)
        # FIXME: there should be a better option than this
        str.gsub("'", "''")
      end

      def build(sql)
        Builder.new(self, sql)
      end

      private

      def run(sql, params)
        if params && params.length > 0
          sql = param_encoder.encode(sql, *params)
        end

        cursor = raw_connection.parse(sql)
        res = cursor.exec

        if block_given?
          yield cursor
        else
          res
        end
      ensure
        cursor.close if cursor
      end
    end
  end
end
