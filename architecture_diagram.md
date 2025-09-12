# 撮合引擎架构图和业务流程

## 系统架构图

```mermaid
graph TB
    subgraph "外部系统"
        Client[客户端/API]
        MQ[RabbitMQ消息队列]
        Redis[(Redis缓存)]
        MySQL[(MySQL数据库)]
        Apollo[Apollo配置中心]
    end

    subgraph "撮合引擎核心模块"
        Main[main.go主程序]
        Config[config配置模块]
        
        subgraph "数据拉取层"
            Puller[puller数据拉取器]
            Validate[validate验证模块]
        end
        
        subgraph "撮合核心"
            Match[match撮合引擎]
            OrderBook[OrderBook订单簿]
            Order[Order订单]
        end
        
        subgraph "市场数据层"
            Market[market市场模块]
            L2Quote[l2quote L2行情]
            Depth[depth深度数据]
        end
        
        subgraph "数据处理"
            Persistence[persistence持久化]
            Snapshotter[snapshotter快照]
            Statistics[statistics统计]
        end
        
        subgraph "调度和监控"
            Scheduler[scheduler调度器]
            Common[common公共模块]
        end
    end

    %% 数据流向
    Client --> MQ
    MQ --> Puller
    Apollo --> Config
    Config --> Main
    Puller --> Validate
    Validate --> Match
    Match --> OrderBook
    OrderBook --> Order
    Match --> Market
    Market --> L2Quote
    L2Quote --> Depth
    Match --> Persistence
    Persistence --> MySQL
    Match --> Snapshotter
    L2Quote --> Redis
    Statistics --> Redis
    Scheduler --> Match

    %% 样式定义
    classDef coreModule fill:#ff9999,stroke:#333,stroke-width:2px
    classDef dataModule fill:#99ccff,stroke:#333,stroke-width:2px
    classDef storageModule fill:#99ff99,stroke:#333,stroke-width:2px
    classDef externalModule fill:#ffcc99,stroke:#333,stroke-width:2px

    class Match,OrderBook,Order coreModule
    class L2Quote,Market,Depth dataModule
    class Redis,MySQL,Persistence,Snapshotter storageModule
    class Client,MQ,Apollo externalModule
```

## 业务流程图

### 订单撮合主流程

```mermaid
sequenceDiagram
    participant C as 客户端
    participant MQ as RabbitMQ
    participant P as Puller
    participant V as Validator
    participant M as Matcher
    participant OB as OrderBook
    participant L2 as L2Quote
    participant R as Redis
    participant DB as MySQL

    C->>MQ: 1. 提交订单
    MQ->>P: 2. 拉取订单消息
    P->>V: 3. 订单验证
    
    alt 验证成功
        V->>M: 4. 发送到撮合引擎
        M->>OB: 5. 查找对手方订单
        
        alt 找到匹配订单
            OB->>M: 6. 返回匹配订单
            M->>M: 7. 执行撮合逻辑
            M->>OB: 8. 更新订单簿
            M->>L2: 9. 生成成交数据
            L2->>R: 10. 更新行情缓存
            M->>DB: 11. 持久化成交记录
            M->>MQ: 12. 发布成交结果
        else 无匹配订单
            M->>OB: 6. 挂单到订单簿
            M->>L2: 7. 更新深度数据
            L2->>R: 8. 更新行情缓存
        end
    else 验证失败
        V->>MQ: 4. 返回错误消息
    end
```

### 系统启动流程

```mermaid
flowchart TD
    Start([系统启动]) --> LoadConfig[加载配置文件]
    LoadConfig --> InitLog[初始化日志系统]
    InitLog --> InitModules[初始化各模块]
    
    InitModules --> InitStats[初始化统计模块]
    InitStats --> InitSnapshot[初始化快照模块]
    InitSnapshot --> InitRedis[初始化Redis客户端]
    InitRedis --> InitRabbitMQ[初始化RabbitMQ]
    InitRabbitMQ --> InitMatch[初始化撮合引擎]
    InitMatch --> InitMarket[初始化市场模块]
    
    InitMarket --> StartExchange[启动交易所]
    StartExchange --> LoadSymbols{加载交易对列表}
    
    LoadSymbols --> |每个交易对| CreateOrderBook[创建订单簿]
    CreateOrderBook --> LoadSnapshot[加载历史快照]
    LoadSnapshot --> InitL2Quote[初始化L2行情]
    InitL2Quote --> InitPuller[初始化数据拉取器]
    InitPuller --> StartMatcher[启动撮合器]
    
    StartMatcher --> Ready[系统就绪]
    Ready --> MainLoop[主循环运行]
```

### 撮合引擎内部流程

```mermaid
flowchart TD
    OrderIn[订单输入] --> TypeCheck{订单类型检查}
    
    TypeCheck --> |限价单| LimitOrder[限价订单处理]
    TypeCheck --> |市价单| MarketOrder[市价订单处理]
    TypeCheck --> |取消单| CancelOrder[取消订单处理]
    
    LimitOrder --> PriceMatch{价格匹配检查}
    MarketOrder --> MarketMatch[市价撮合]
    CancelOrder --> FindCancel[查找待取消订单]
    
    PriceMatch --> |有匹配| ExecuteTrade[执行成交]
    PriceMatch --> |无匹配| AddToBook[加入订单簿]
    
    MarketMatch --> ExecuteTrade
    FindCancel --> RemoveOrder[移除订单]
    
    ExecuteTrade --> UpdateBook[更新订单簿]
    AddToBook --> UpdateDepth[更新深度数据]
    RemoveOrder --> UpdateDepth
    
    UpdateBook --> UpdateDepth
    UpdateDepth --> PublishResult[发布结果]
    PublishResult --> Snapshot{是否需要快照}
    
    Snapshot --> |是| SaveSnapshot[保存快照]
    Snapshot --> |否| Complete[完成处理]
    SaveSnapshot --> Complete
```

## 数据结构关系图

```mermaid
erDiagram
    OrderBook ||--o{ Order : contains
    Order {
        int64 SeqId
        int64 UserId
        int64 OrderId
        OrderBuyOrSell BuyOrSell
        OrderType Type
        OrderState State
        decimal Price
        decimal UnfilledAmount
        decimal AccfilledAmount
        int64 CreateAt
    }
    
    OrderBook {
        string Symbol
        int64 FromId
        map cache
        TreeSet BuySet
        TreeSet SellSet
    }
    
    MatchResult ||--o{ Trade : contains
    Trade {
        int64 TradeId
        decimal Price
        decimal Amount
        int64 TakerOrderId
        int64 MakerOrderId
        int64 Timestamp
    }
    
    L2Quote ||--o{ DepthLevel : contains
    DepthLevel {
        decimal Price
        decimal Amount
        int OrderCount
    }
    
    Snapshot ||--|| OrderBook : saves
    Snapshot {
        int64 SnapshotId
        string Symbol
        int64 Timestamp
        bytes Data
    }
```

## 关键组件说明

### 1. 撮合引擎核心 (match包)
- **OrderBook**: 订单簿，使用红黑树存储买卖订单
- **Order**: 订单结构，包含价格、数量、类型等信息
- **Matcher**: 撮合逻辑，处理订单匹配和成交

### 2. 市场数据 (market包)
- **L2Quote**: L2级别行情数据生成
- **Depth**: 市场深度数据管理
- **Kline**: K线数据生成和管理

### 3. 数据处理
- **Puller**: 从消息队列拉取订单数据
- **Persistence**: 数据持久化到数据库
- **Snapshotter**: 订单簿快照管理

### 4. 基础设施
- **Redis**: 缓存行情数据和统计信息
- **RabbitMQ**: 订单消息队列和结果发布
- **MySQL**: 持久化存储成交记录

### 5. 监控和调度
- **Statistics**: 系统统计信息
- **Scheduler**: 定时任务调度
- **Validate**: 订单验证 