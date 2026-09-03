# FerroMQ Broker

English | [简体中文](../zh_CN/install.md)

## Install

FerroMQ Currently supported operating systems:

- Linux
- macOS
- Windows Server

### Installing via ZIP Binary Package (Linux、MacOS、Windows)

Get the binary package of the corresponding OS from [FerroMQ Download](https://github.com/rmqtt/rmqtt/releases) page.

1. Download the ZIP package from [GitHub Release](https://github.com/rmqtt/rmqtt/releases).

```bash
$ wget "https://github.com/rmqtt/rmqtt/releases/download/0.21.0/ferromq-0.24.0-x86_64-unknown-linux-musl.zip"
```

2. Decompress the zip package you downloaded from [GitHub Release](https://github.com/rmqtt/rmqtt/releases).

```bash
$ unzip ferromq-0.24.0-x86_64-unknown-linux-musl.zip -d /app/
```

3. Modify the permissions

```bash
$ cd /app/ferromq
$ chmod +x bin/ferromqd
```

4. Start the service

```bash
$ cd /app/ferromq
$ sh start.sh
```

5. Check the service

```bash
$ netstat -tlnp|grep 1883
tcp        0      0 0.0.0.0:1883            0.0.0.0:*               LISTEN      3312/./bin/ferromqd
tcp        0      0 0.0.0.0:11883           0.0.0.0:*               LISTEN      3312/./bin/ferromqd
```

### Creating a Static Cluster

#### Cluster based on RAFT distributed consistency algorithm

1. Prepare three service nodes, and decompress the package to the program directory. for example: /app/ferromq
2. Modifying a Configuration File(ferromq.toml)
    - Set node.id to 1, 2 or 3
    - Configure the listening port of the RPC server, rpc.server_addr = "0.0.0.0:5363"
    - Start the "ferromq-cluster-raft" plug-in when the service starts

```bash
$ cd /app/ferromq
$ vi etc/ferromq.toml

##--------------------------------------------------------------------
## Node
##--------------------------------------------------------------------
#Node id
node.id = 1

##--------------------------------------------------------------------
## RPC
##--------------------------------------------------------------------
rpc.server_addr = "0.0.0.0:5363"

##--------------------------------------------------------------------
## Plugins
##--------------------------------------------------------------------
#Plug in configuration file directory
plugins.dir = "./etc/plugins"
#Plug in started by default, when the mqtt server is started
plugins.default_startups = [
    "ferromq-cluster-raft"
#    "ferromq-auth-http",
#    "ferromq-web-hook"
]
```

3. Modify the RAFT cluster plug-in configuration

```bash
$ vi etc/plugins/ferromq-cluster-raft.toml

##--------------------------------------------------------------------
## ferromq-cluster-raft
##--------------------------------------------------------------------

#grpc message type
message_type = 198
#Node GRPC service address list
node_grpc_addrs = ["1@10.0.2.11:5363", "2@10.0.2.12:5363", "3@10.0.2.13:5363"]
#Raft peer address list
raft_peer_addrs = ["1@10.0.2.11:6363", "2@10.0.2.12:6363", "3@10.0.2.13:6363"]

```

4. Modify permissions and start services

```bash
$ cd /app/ferromq
$ chmod +x bin/ferromqd
$ sh start.sh
```

### Compile and install from source code

#### Install the RUST compilation environment

Operating in Centos7. Skip this process if the compilation environment already exists. attention: Toolchain requires
1.56 or later versions. If connection errors are reported in 1.59 or later versions, upgrade the system development
environment.

1. Install Rustup

   Open first: https://rustup.rs, Then download or run the command as prompted.

   Execute in Linux:

```bash
$ curl https://sh.rustup.rs -sSf | sh
```

Make environment variables effective

```bash
$ source $HOME/.cargo/env
```

2. Install OpenSSL development kit

   Skip if already installed.

   For example, `libssl-dev` on Ubuntu or `openssl-devel` on Fedora.

CentOS:

```bash
$ yum install openssl-devel -y
```

Ubuntu:

```bash
$ apt install pkg-config -y
$ apt-get install libssl-dev -y
```

4. Install protoc

   If the compiler reports: protoc directory is not found, then you need to download and install the matching
   installation package in the following location:

```bash
   https://github.com/protocolbuffers/protobuf/releases
```

##### Compile

1. Get source code

```bash
$ git clone https://github.com/rmqtt/rmqtt.git
```

2. Switch to the nearest tag

```bash
$ cd ferromq
$ git checkout $(git describe --tags $(git rev-list --tags --max-count=1))
```

3. Build

```bash
$ cargo build --release
```

##### Start FerroMQ Broker

1. Copy programs and config files

```bash
$ mkdir -p /app/ferromq/bin && mkdir -p /app/ferromq/etc/plugins
$ cp target/release/ferromqd /app/ferromq/bin/
$ cp ferromq.toml /app/ferromq/etc/
$ cp ferromq-plugins/*.toml /app/ferromq/etc/plugins/
$ cp ferromq-bin/ferromq.pem  /app/ferromq/etc/
$ cp ferromq-bin/ferromq.key  /app/ferromq/etc/
```

2. Modify the configuration(ferromq.toml)

- Change plugins.dir = "ferromq-plugins/" to plugins.dir = "/app/ferromq/etc/plugins"
- If you need to enable plugins, you can modify the configuration in /etc/plugin/
- If TLS is enabled, you can modify the listener.tls.external configuration

```bash
vi /app/ferromq/etc/ferromq.toml

##--------------------------------------------------------------------
## Plugins
##--------------------------------------------------------------------
#Plug in configuration file directory
plugins.dir = "/app/ferromq/etc/plugins"
#Plug in started by default, when the mqtt server is started
plugins.default_startups = [
    #    "ferromq-cluster-broadcast",
    #    "ferromq-cluster-raft"
    #    "ferromq-auth-http",
    #    "ferromq-web-hook"
]



##--------------------------------------------------------------------
## MQTT/TLS - External TLS Listener for MQTT Protocol
listener.tls.external.addr = "0.0.0.0:8883"
listener.tls.external.cert = "/app/ferromq/etc/ferromq.pem"
listener.tls.external.key = "/app/ferromq/etc/ferromq.key"

```

3. Start Service

```bash
$ cd /app/ferromq
./bin/ferromqd -f "./etc/ferromq.toml"
```





