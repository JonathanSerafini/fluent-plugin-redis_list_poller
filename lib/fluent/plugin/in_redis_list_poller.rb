require "fluent/plugin/input"
require "fluent/plugin_mixin/redis"

module Fluent
  module Plugin
    class RedisListPollerInput < Input
      include Fluent::PluginMixin::Redis

      Plugin.register_input('redis_list_poller', self)

      helpers :storage

      # redis list details
      # - command: redis command to execute when fetching messages
      # - batch_size: if greater than 0, fetch messages in batches
      config_param :command,    :string,  :default => "lpop"
      config_param :batch_size, :integer, :default => 0

      # input plugin parameters
      config_param :tag,      :string,  :default => nil
      config_param :format,   :string,  :default => "json"

      # Initialize new input plugin
      # @since 0.1.0
      # @return [NilClass]
      def initialize
        super
        require 'cool.io'
        require 'msgpack'
      end

      # Initialize attributes and parameters
      # @since 0.1.0
      # @return [NilClass]
      def configure(config)
        super

        configure_params(config)
        configure_parser(config)
        configure_locking(config)

        @retry_at     = nil
      end

      # Configure plugin parameters
      # @since 0.1.0
      # @return [NilClass]
      def configure_params(config)
        %w(host port key command format tag).each do |key|
          next if instance_variable_get("@#{key}")
          raise Fluent::ConfigError, "configuration key missing: #{key}"
        end

        unless %w(lpop rpop).include?(@command)
          raise Fluent::ConfigError, "command must be either lpop or rpop"
        end
      end

      # Configure record parser
      # @since 0.1.0
      # @return [NilClass]
      def configure_parser(config)
        @parser = Plugin.new_parser(@format)
        @parser.configure(config)
      end

      # Configure locking
      # @since 0.1.0
      # @return [NilClass]
      def configure_locking(config)
        @storage  = storage_create(type: 'local')
        @lock_key = "fluentd:#{@key}:lock"
      end

      # Prepare the plugin event loop
      #
      # This method will initialize the Redis connection object, create any required Redis structures as well
      # as define and begin the event pollers.
      #
      # @since 0.1.0
      # @return [NilClass]
      def start
        super

        @loop = Coolio::Loop.new
        start_redis
        start_poller
        @thread = Thread.new(&method(:run))
      end

      # Prepare the Redis queue poller
      #
      # This timed event will routinely poll items from the Redis list and
      # emit those through the pipeline.
      #
      # @since 0.1.0
      # @return [NilClass]
      def start_poller
        @poller = TimerWatcher.new(
          @poll_interval,
          log,
          &method(:action_poll)
        )

        @lock_monitor = TimerWatcher.new(
          1,
          log,
          &method(:action_locking_monitor)
        )

        @loop.attach(@poller)
        @loop.attach(@lock_monitor)
      end

      # Begin the logging pipeline
      # @since 0.1.0
      # @return [NilClass]
      def run
        @loop.run
      rescue => e
        log.error "unexpected error", :error => e
        log.error_backtrace
      end

      # Tear down the plugin
      # @since 0.1.0
      # @return [NilClass]
      def shutdown
        @loop.watchers.each { |w| w.detach }
        @loop.stop
        Thread.kill(@thread)
        @thread.join
        shutdown_redis
        super
      end

      # Whether to fetch a single item or a multiple items in batch
      # @since 0.1.0
      # @return [TrueClass, FalseClass]
      def batched?
        @batch_size and @batch_size > 1
      end

      # Wether the poller has been temporarily disabled or should fetch messages
      # been temporarily disabled
      # @since 0.1.0
      # @return [TrueClass, FalseClass]
      def sleeping?
        @retry_at and @retry_at >= Engine.now
      end

      # Whether the poller has been locked
      # @since 0.1.0
      # @return [TrueClass, FalseClass]
      def locked?
        @storage.get(@lock_key)
      end

      # Set a sleep delay, ensuring that we will not attempt to fetch messages
      # @since 0.1.0
      # @param [Integer] delay, the amount of seconds to wait
      # @return [Integer] timestamp when this expires
      def sleep!(delay = @sleep_interval)
        @retry_at = Engine.now + delay
      end

      # Poll messages from the redis server in either single message or 
      # batch mode.
      # @since 0.1.0
      # @param [&block] the block to yield single messages to
      # @return [NilClass]
      def poll_messages
        commands = []

        if batched?
          @redis.pipelined do
            @batch_size.times do
              commands << @redis.call(@command, @key)
            end
          end
        else
          commands << @redis.call(@command, @key)
        end

        commands.each do |command|
          yield command.is_a?(Redis::Future) ? command.value : command
        end
      end

      # Action to execute when polling for the lock key
      # @since 0.1.0
      # @return [NilClass]
      def action_locking_monitor
        lock_value = @redis.get(@lock_key)
        @storage.put(@lock_key, lock_value)
      end

      # Action to execute when the poller event watcher executes
      #
      # Given that the watcher is pretty lightweight, we simply return if the
      # worker has been set to sleep instead of actually sleeping. Doing
      # otherwise seemed to cause locking.
      #
      # Otherwise we iterate through messages, parse and emit them. 
      #
      # @since 0.1.0
      # @return [NilClass]
      def action_poll
        now = Engine.now
        messages = []

        if sleeping?
          log.trace "redis worker is sleeping"
          return
        end

        if locked?
          log.trace "redis queue is locked"
          return
        end

        poll_messages do |message|
          if message.nil?
            log.debug "redis queue is empty"
            sleep!(@sleep_interval)
            break
          end

          @parser.parse(message) do |time, record|
            if time && record
              router.emit @tag || @key, time || Engine.now, record
            else
              log.warn "failed to parse message: #{message}"
            end
          end
        end
      rescue => e
        log.error "error fetching record", :error => e
        log.error_backtrace
        sleep!(@retry_interval)
      end

      # Generic Cool.io timer which will execute a given callback on schedule.
      # @since 0.1.0
      class TimerWatcher < Coolio::TimerWatcher
        attr_reader :log

        def initialize(interval, log, &callback)
          @callback = callback
          @log = log
          super(interval, true)
        end

        def on_timer
          @callback.call
        rescue => e
          log.error "unexpected error", :error => e
          log.error_backtrace
        end
      end
    end
  end
end
