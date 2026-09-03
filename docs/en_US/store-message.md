English | [简体中文](../zh_CN/store-message.md)


# Store unexpired messages

Messages that are published will be stored until they expire. As long as a message remains unexpired, any subscriptions made to the corresponding topic after the message is published will be forwarded. Messages will be automatically cleared once they expire.

#### Plugin:

```bash
ferromq-message-storage
```

#### Plugin configuration file:

```bash
plugins/ferromq-message-storage.toml
```

#### Plugin configuration options:

```bash
##--------------------------------------------------------------------
## ferromq-message-storage
##--------------------------------------------------------------------

##ram, redis, redis-cluster
storage.type = "ram"

##ram
storage.ram.cache_capacity = "3G"
storage.ram.cache_max_count = 1_000_000
storage.ram.encode = false

##Maximum pending messages in the in-memory channel (back-pressure limit).
##Default: 300000
#storage.ram.queue_max = 300_000

##redis
storage.redis.url = "redis://127.0.0.1:6379/"
storage.redis.prefix = "message-{node}"

##redis-cluster
storage.redis-cluster.urls = ["redis://127.0.0.1:6380/", "redis://127.0.0.1:6381/", "redis://127.0.0.1:6382/"]
storage.redis-cluster.prefix = "message-{node}"

##Quantity of expired messages cleared during each cleanup cycle.
cleanup_count = 5000

##Timeout for storage I/O operations, channel sends, and circuit breaker
##per-operation timeout. 0 = no timeout. Examples: "5s", "500ms".
##Default: "15s"
#backend_timeout = "15s"

##─── Circuit breaker ────────────────────────────────────────────────────────
##All circuit-breaker parameters (failure rate, window, etc.) are inherited
##from the global `[circuit_breaker]` section in `ferromq.toml`.
##The per-operation timeout uses `backend_timeout` above.
```

Currently, three storage engines are supported: "ram," "redis," and "redis-cluster." "ram" is stored in local memory and
can be configured with maximum memory usage or maximum message count, and it can specify whether messages should be encoded
before storage. Prefix configuration allows different FerroMQ nodes to use the same Redis storage service. `{node}` will be
replaced by the identifier of the current node.

`backend_timeout` (default: `"15s"`) configures the timeout for storage I/O operations, channel sends, and the circuit breaker per-operation timeout. Set to `"0s"` for no timeout.

The Circuit Breaker parameters (failure rate threshold, sliding window type/size, minimum calls, OPEN duration, slow call threshold, etc.) are inherited from the global `[circuit_breaker]` section in `ferromq.toml`. The per-operation timeout uses the `backend_timeout` setting.


By default, this plugin is not enabled. To activate it, you must add the `ferromq-message-storage` entry to the
`plugins.default_startups` configuration in the main configuration file `ferromq.toml`, as shown below:
```bash
##--------------------------------------------------------------------
## Plugins
##--------------------------------------------------------------------
#Plug in configuration file directory
plugins.dir = "ferromq-plugins/"
#Plug in started by default, when the mqtt server is started
plugins.default_startups = [
    #"ferromq-retainer",
    #"ferromq-auth-http",
    #"ferromq-cluster-broadcast",
    #"ferromq-cluster-raft",
    #"ferromq-sys-topic",
    "ferromq-message-storage",
    #"ferromq-session-storage",
    "ferromq-web-hook",
    "ferromq-http-api"
]
```










