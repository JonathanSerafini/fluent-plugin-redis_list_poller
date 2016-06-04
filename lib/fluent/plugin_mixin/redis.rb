
module Fluent
  module PluginMixin
    module Redis
      def self.included(base)
        base.class_eval do
          # redis connection details
          config_param :host,     :string,  :default => '127.0.0.1'
          config_param :port,     :integer, :default => 6379
          config_param :path,     :string,  :default => nil
          config_param :password, :string,  :default => nil
          config_param :db,       :integer, :default => 0
          config_param :timeout,  :float,   :default => 5.0
          config_param :driver,   :string,  :default => "ruby"

          # redis list details
          # - key: redis key of type `list` to fetch messages from
          # - command: redis command to execute when fetching messages
          # - batch_size: if greater than 0, fetch messages in batches
          config_param :key,        :string,  :default => nil

          # worker parameters
          # - poll_inteval: interval between message polling actions
          #                 *NOTE*: Apparently this must be greather than 0
          # - sleep_interval: interval to wait after receiving 0 messages
          # - retry_interval: interval to wait before retrying after an error
          config_param :poll_interval,    :float, :default => 1
          config_param :sleep_interval,   :float, :default => 5
          config_param :retry_interval,   :float, :default => 5

          attr_reader  :redis
        end
      end

      # Initialize new input plugin
      # @since 0.1.0
      # @return [NilClass]
      def initialize
        require 'redis'
        super
      end

      # Prepare the Redis conncection object
      # @since 0.1.0
      # @return [Redis]
      def start_redis
        @redis  = ::Redis.new(
          :host => @host,
          :port => @port,
          :driver => @driver,
          :thread_safe => true
        )
      end

      # Destroy the Redis connection object
      # @since 0.1.0
      # @return [NilClass]
      def shutdown_redis
        @redis.quit
      end
    end
  end
end
