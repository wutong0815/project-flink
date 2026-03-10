# Akka 集成使用指南

## 概述

flink-core 模块已成功集成 Akka，提供了 Actor 模型和流式处理能力。

## 依赖配置

已在父 pom.xml 和 flink-core/pom.xml 中添加以下依赖：

- `akka-actor_2.12`: Akka Actor 核心库
- `akka-stream_2.12`: Akka Stream 流处理库
- `akka-testkit_2.12`: Akka 测试工具（测试作用域）

## 核心类说明

### 1. AkkaClientUtil

Akka 工具类，提供 ActorSystem 管理和 Actor 创建功能。

**主要方法：**
- `getActorSystem()`: 获取 ActorSystem 实例
- `createActor(Props, String)`: 创建 Actor
- `stopActor(ActorRef)`: 停止 Actor
- `shutdown()`: 关闭 ActorSystem
- `isRunning()`: 检查 ActorSystem 是否运行中

**使用示例：**
```java
// 创建 ActorSystem
AkkaClientUtil client = new AkkaClientUtil("my-system");

// 创建 Actor
Props props = Props.create(MyActor.class, MyActor::new);
ActorRef actorRef = client.createActor(props, "my-actor");

// 发送消息
actorRef.tell(new Message("Hello"), ActorRef.noSender());

// 关闭系统
client.shutdown();
```

### 2. ExampleActor

演示如何创建和使用 Akka Actor。

**特点：**
- 定义了 Message 和 Response 消息类
- 实现消息处理和回复逻辑
- 处理未知消息类型

### 3. ExampleStream

演示如何使用 Akka Stream 进行流式数据处理。

**示例功能：**
- **简单示例**: Source → Flow → Sink 基础流处理
- **背压示例**: 异步处理与背压控制
- **窗口聚合**: 时间窗口和数量窗口分组

### 4. AkkaExample

综合示例，展示 Actor 和 Stream 的完整用法。

**运行方式：**
```java
AkkaExample example = new AkkaExample();
example.runFullExample(); // 运行所有示例
```

## 使用方法

### 通过命令行运行

启动 FlinkCore 应用，使用以下命令：

```bash
# 初始化 Akka 客户端（不执行操作）
java -jar flink-core.jar akka [systemName]

# 运行 Akka 示例
java -jar flink-core.jar akka-example
```

### 在代码中使用

#### 1. 使用 Actor

```java
import com.example.project.core.akka.AkkaClientUtil;
import com.example.project.core.akka.ExampleActor;
import akka.actor.ActorRef;
import akka.actor.Props;

// 创建 ActorSystem
AkkaClientUtil client = new AkkaClientUtil("demo-system");

try {
    // 创建 Actor
    Props props = Props.create(ExampleActor.class, ExampleActor::new);
    ActorRef actorRef = client.createActor(props, "example-actor");
    
    // 发送消息
    ExampleActor.Message message = new ExampleActor.Message("Hello, Akka!", actorRef);
    actorRef.tell(message, ActorRef.noSender());
    
    // 等待处理完成
    Thread.sleep(1000);
} finally {
    client.shutdown();
}
```

#### 2. 使用 Stream

```java
import com.example.project.core.akka.ExampleStream;

ExampleStream stream = new ExampleStream();

try {
    // 运行简单示例
    stream.runSimpleExample();
    
    // 运行背压示例
    stream.runBackpressureExample();
    
    // 运行窗口聚合示例
    stream.runWindowingExample();
    
    Thread.sleep(2000);
} finally {
    stream.shutdown();
}
```

## 示例说明

### Actor 示例输出

```
=== 运行 Akka Actor 示例 ===
ActorSystem 'actor-example-system' 创建成功
Actor 'example-actor' 创建成功
发送消息给 Actor...
收到消息：Hello, Akka!
处理消息：已处理：Hello, Akka!
Actor 示例执行完成
正在关闭 ActorSystem 'actor-example-system'...
ActorSystem 'actor-example-system' 已关闭
```

### Stream 示例输出

```
=== 运行 Akka Stream 示例 ===
运行 Akka Stream 简单示例...
计算结果：24
示例执行完成
Stream 示例执行完成
```

## 与 Flink 集成

Akka 可以与 Flink 结合使用，提供更丰富的并发模型：

### 场景 1：Flink 作业监控

使用 Akka Actor 接收 Flink 作业状态更新并进行处理。

### 场景 2：事件驱动处理

Akka Actor 作为事件源，将数据发送到 Flink 流处理管道。

### 场景 3：分布式协调

利用 Akka Cluster 实现多个 Flink 作业之间的协调。

## 注意事项

1. **资源管理**: 使用完毕后务必调用 `shutdown()` 关闭 ActorSystem
2. **异常处理**: Actor 中的异常会被 SupervisorStrategy 处理
3. **消息不可变**: 所有消息类都应该是不可变的
4. **背压处理**: 使用 Akka Stream 时注意背压策略配置
5. **Scala 版本**: 当前使用 Scala 2.12，与 Flink 保持一致

## 参考资料

- [Akka 官方文档](https://doc.akka.io/docs/akka/current/)
- [Akka Stream 编程指南](https://doc.akka.io/docs/akka/current/stream/index.html)
- [Akka Actor 规范](https://doc.akka.io/docs/akka/current/typed/index.html)
