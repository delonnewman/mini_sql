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
        
      end

      def exec(sql, *params)
        run(sql, params)
      end

      private

      def run(sql, params)
        if params && params.length > 0
          sql = param_encoder.encode(sql, *params)
        end
        raw_connection.exec(sql)
      end
    end
  end
end
