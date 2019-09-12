# Abandoned Project

**NOTE**: This project is no longer being maintained.

# Fluent::Plugin::RedisListPoller

This gem provides a few input plugins for Fluentd which are designed to more efficiently poll large volumes of messages than other existing redis input plugins. 

* *redis_list_poller*: Input plugin designed to fetch large volumes of messages
* *redis_list_monitor*: Input plugin designed to fetch queue size metrics

In additional to the standard stuff, the `redis_list_poller` input plugin also looks for a lock key in Redis to see whether it has been administratively disabled. This lock key follows the naming convention of `redis:KEY_NAME:lock`.

## Requirements

* Fluentd v0.14+

## Installation

```bash
gem install fluent-plugin-redis_list_poller
```

## Configuration Options

```
<source>
  @type       redis_list_poller
  host        127.0.0.1
  port        6379
  path        nil
  password    nil
  db          0
  timeout     5.0
  driver      ruby

  key         redis_list_item
  command     lpop
  batch_size  100

  tag         redis.data

  poll_interval      0.01
  sleep_interval     5
  retry_interval    5

  <parse>
    @type json
  </parse>
</source>
```

## Benchmarks

These very simple benchmarks which give a rough ideas of the sorts of message rates one can expect from this input plugin.

They were run within a Vagrant box, with a local Redis, running on my Macbook Pro 15'. Fluentd is running on a single CPU core and is using the examples/standalone.conf.

poll_interval|batch_size|messages/second|
-------------|----------|---------------|
1     | 1   | 1
0.5   | 1   | 2
0.2   | 1   | 5
0.01  | 1   | 100
0.001 | 1   | 457
poll_interval|batch_size|messages/second|
1     | 10  | 10
0.5   | 10  | 20
0.2   | 10  | 50
0.01  | 10  | 1000 
0.001 | 10  | 1793
poll_interval|batch_size|messages/second|
1     | 100 | 100
0.5   | 100 | 200
0.2   | 100 | 500
0.01  | 100 | 7823
0.001 | 100 | 7600

