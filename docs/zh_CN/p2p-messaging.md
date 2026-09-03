[English](../en_US/p2p-messaging.md)  | 简体中文

# P2P 消息传递

*ferromq-p2p-messaging* 插件用于支持点对点（P2P）消息传递机制。
客户端可以通过特定的主题规则直接向目标客户端发送消息，而无需目标客户端显式订阅。

该插件会根据配置的规则自动解析主题中的 `clientid`，并将消息投递到对应客户端。

---

#### 配置 P2P 消息规则

P2P 消息的主题格式由插件配置决定，支持三种模式：

* **prefix 模式**：`p2p/{clientid}/{topic}`
* **suffix 模式**：`{topic}/p2p/{clientid}`
* **both 模式**：同时支持 `prefix` 和 `suffix` 两种格式

其中：

* `{clientid}` 表示目标客户端 ID。
* `{topic}` 表示实际的业务主题。

示例：

* `p2p/device001/sensor/temp`  → 发送给 `device001` 的主题 `sensor/temp`
* `sensor/temp/p2p/device001` → 发送给 `device001` 的主题 `sensor/temp`

---

#### 插件：

```bash
ferromq-p2p-messaging
```

---

#### 插件配置文件：

```bash
plugins/ferromq-p2p-messaging.toml
```

---

#### 插件配置项：

```bash
##--------------------------------------------------------------------
## ferromq-p2p-messaging
##--------------------------------------------------------------------

#"prefix": `p2p/{clientid}/{topic}` — the `p2p` identifier is at the start.
#"suffix": `{topic}/p2p/{clientid}` — the `p2p` identifier is at the end.
#"both": supports both prefix and suffix formats.
mode = "prefix"
```

---

#### 启用插件

默认情况下此插件并不会启动，如果要开启此插件，必须在主配置文件 `ferromq.toml` 中的 `plugins.default_startups` 配置中添加 `ferromq-p2p-messaging` 项，如：

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
    #"ferromq-message-storage",
    #"ferromq-session-storage",
    "ferromq-p2p-messaging",
    "ferromq-web-hook",
    "ferromq-http-api"
]
```

