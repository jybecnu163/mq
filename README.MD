#   mymq —— 轻量级消息中间件
my-mq 是一个用 Java 编写的学习型消息中间件，受 QMQ 的设计启发，实现了其核心特性：
消息持久化、任意延时消息、长轮询和基于 Tag 的服务端过滤。项目使用 Netty 作为网络框架，
以本地文件系统作为存储，无外部依赖，便于理解和二次开发。

##   ✨ 核心特性
消息持久化：消息顺序追加写入日志文件，支持快速随机读取，重启不丢失。
独立消费者组：每个 Topic + ConsumerGroup 拥有独立的消费索引和进度，互不干扰。
Pull + 长轮询：消费者主动拉取，无消息时挂起连接，新消息到达后毫秒级推送。
任意延时消息：两级时间轮（磁盘按时分片持久化 + 内存时间轮），支持任意时长的延迟，重启后未到期消息自动恢复。
服务端 Tag 过滤：消息可携带标签，消费者按需订阅，不匹配的消息自动跳过，减少无效传输。

##   📁 项目结构
```text
my-mq/
├── pom.xml                          # 父 POM，管理模块与依赖版本
├── my-mq-common/                    # 公共模块：协议定义、消息编解码
├── my-mq-broker/                    # 服务端：存储引擎、消费管理、延时调度
├── my-mq-client/                    # 客户端：Producer、Consumer、MQClient
└── my-mq-example/                   # 示例入口：Broker 启动类、客户端示例
```

##   🧱 系统架构
```text
Client (Producer / Consumer)
│  Netty 长连接 (自定义二进制协议)
│
Broker (单进程)
├── MessageHandler          # 处理 SEND / PULL / ACK / 延时发送
├── BrokerStore (单例)      # 存储调度中枢
│   ├── MessageLog          # 消息日志文件
│   ├── ConsumeIndexManager # 消费者索引与进度
│   └── DelayMessageScheduler # 两级时间轮
└── 长轮询挂起管理
```
所有数据默认存储于启动目录下的 data/ 文件夹中。

##   🚀 快速开始
环境要求：JDK 17+、Maven 3.6+

1. 构建项目
   ```bash
   cd my-mq
   mvn clean compile
   ```
2. 启动 Broker
   ```bash
   mvn -pl my-mq-example exec:java -Dexec.mainClass="com.mymq.example.ExampleBrokerMain"
   ```
   控制台输出 Broker started on port 8080 表示启动成功。

3. 运行客户端示例
   打开另一个终端，执行：

```bash
mvn -pl my-mq-example exec:java -Dexec.mainClass="com.mymq.example.ExampleClientMain"
```
示例中会启动一个 Producer 持续发送消息，两个 Consumer（group-1、group-2）独立消费，同时演示 Tag 过滤和延时消息。

##   📡 协议支持
    命令	方向	说明
    SEND	Client→Broker	发送普通/延时消息（通过 delayMs 区分）
    PULL	Client→Broker	拉取消息（支持长轮询和 Tag 过滤）
    ACK	Client→Broker	确认消息消费，推进消费进度
    RESPONSE	Broker→Client	所有请求的响应（含成功/错误信息）

##   ⚙️ 主要配置
    当前所有配置均硬编码在代码中，主要参数：
    
    参数	位置	默认值	说明
    Broker 端口	BrokerServer	8080	服务监听端口
    数据目录	BrokerServer	./data	消息与索引存储路径
    长轮询超时	MessageHandler	5000 ms	无消息时挂起时长
    未来可抽取为外部配置文件（如 config.yaml）。

## ✅ 已实现功能清单
🧱 基础通信与协议
自定义二进制协议（基于 Netty），支持 SEND、PULL、ACK、RESPONSE 命令。
LengthFieldBasedFrameDecoder 解决 TCP 粘包/拆包。
客户端异步请求：CompletableFuture 实现请求-响应匹配。

💾 消息持久化
顺序追加写日志（MessageLog），消息携带长度头，支持根据物理偏移量随机读取。
消费者索引与进度管理（ConsumeIndexManager），每个 Topic+ConsumerGroup 维护独立索引和进度，持久化到磁盘。
重启后消费进度不丢失，支持断点续传。

📬 消费模型
Pull 模型：消费者主动拉取，服务端返回下一条消息。
长轮询（Long Polling）：无消息时挂起连接，新消息到达后毫秒级推送，超时 5 秒返回空响应。
多消费者组独立消费：每个组独立维护索引和进度，互相不干扰。
消费确认（ACK）：顺序确认，只有当 ACK 偏移严格递增时才推进进度。

⏱️ 延时消息（核心亮点）
两级时间轮：
磁盘时间轮：消息按到期时间的小时分片持久化，支持任意时长的延时（如数天）。
内存时间轮：基于 Netty HashedWheelTimer，毫秒级精度调度当前小时内的消息。
重启恢复：Broker 启动时自动扫描未到期的磁盘延时文件，重新加载到内存时间轮，保证消息不丢。
到期后消息自动转入普通消息队列，触达长轮询消费者。

🏷️ 基于 Tag 的服务端过滤
生产者发送消息时可携带 Tag（如 "paid,order"）。
消费者拉取时可指定订阅 Tag（如 "paid" 或 "*" 匹配所有）。
服务端在拉取时自动跳过不匹配的消息，避免无效数据传输。

🛡️ 错误处理与连接管理
连接断开时自动清理挂起的长轮询请求，防止内存泄漏。
ACK 请求有明确响应，防止客户端永久阻塞。
异常捕获与日志输出，避免无响应死锁。

📡 监控与指标
内置MetricsServer供prometheus抓取
包含dockerfile和compose文件，供一件启动
单生产者写10W消息约9s
10生产者分别写10W消息约28s，tps~=3.5W

##   🗺️ 未来计划
    事务消息（与本地数据库事务绑定）
    消息重试与死信队列
    集群化（多 Broker、分区、副本）
    可视化管理控制台

##   📄 许可
本项目仅供学习交流使用。