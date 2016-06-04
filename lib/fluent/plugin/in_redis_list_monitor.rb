require "fluent/plugin/input"
require "fluent/plugin_mixin/redis"

module Fluent
  module Plugin
    # Input plugin which will monitor the size of a redis list and periodically
    # output metrics to the login pipeline.
    # @since 0.1.0
    class RedisListMonitorInput < Input
      include Fluent::PluginMixin::Redis

      Plugin.register_input('redis_list_monitor', self)

      # input plugin parameters
      config_param :tag,      :string,  :default => nil

      # Initialize new input plugin
      # @since 0.1.0
      # @return [NilClass]
      def initialize
        super
        require 'cool.io'
      end

      # Initialize attributes and parameters
      # @since 0.1.0
      # @return [NilClass]
      def configure(config)
        super

        configure_params(config)
        configure_locking(config)

        @queue_length = 0
        @retry_at     = nil
      end

      # Configure plugin parameters
      # @since 0.1.0
      # @return [NilClass]
      def configure_params(config)
        %w(host port key tag).each do |key|
          next if instance_variable_get("@#{key}")
          raise Fluent::ConfigError, "configuration key missing: #{key}"
        end
      end

      # Configure locking
      # @since 0.1.0
      # @return [NilClass]
      def configure_locking(config)
        @storage  = storage_create(type: 'local')
        @lock_key = "redis:#{@key}:lock"
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

      def start_poller
        @poller = TimerWatcher.new(
          @poll_interval,
          log,
          &method(:action_poll)
        )

        @loop.attach(@poller)
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

      # Wether the poller has been temporarily disabled or should fetch messages
      # been temporarily disabled
      # @since 0.1.0
      # @return [TrueClass, FalseClass]
      def sleeping?
        @retry_at and @retry_at >= Engine.now
      end

      # Set a sleep delay, ensuring that we will not attempt to fetch messages
      # @since 0.1.0
      # @param [Integer] delay, the amount of seconds to wait
      # @return [Integer] timestamp when this expires
      def sleep!(delay = @sleep_interval)
        @retry_at = Engine.now + delay
      end

      # Action to execute when the monitor event watcher executes
      #
      # The monitor is simply responsible for outputting the queue length to 
      # the logs as well as detecting zero length lists. 
      #
      # @since 0.1.0
      # @return [NilClass]
      def action_poll
        now = Engine.now

        if sleeping?
          log.trace "redis worker is sleeping"
          return
        end

        list_size = @redis.llen(@key)

        event = {
          "timestamp" => now,
          "message" => "redis queue monitor",
          "hostname" => @host,
          "key" => @key,
          "size" => list_size
        }

        router.emit @tag, now, event
      rescue => e
        log.error "error monitoring queue", :error => e
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
          log.error "unexpected error", :error=>e
          log.error_backtrace
        end
      end
    end
  end
end
