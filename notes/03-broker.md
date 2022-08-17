# Broker

## 工作流程

### **ZooKeeper存储的Kafka信息**

```bash
# zookeeper命令行
bin/zookeeper-shell.sh localhost:2181
# 查看根节点信息
ls /
# 查看kafka信息
ls /kafka
```

GUI工具：prettyZoo

- /kafka
  - /admin
  - /brokers
    - /ids 记录有哪些服务器
    - /topics/[topic]/partitions/0/state 记录对应主题对应分区的状态: leader是谁，有哪些服务器可用
  - /cluster
  - /consumers 0.9版本之前保存offset信息，之后offset存储在kafka主题中
  - /controller 辅助选举leader
  - /config
  - /Controller_epoch
  - /isr_change_notification
  - /latest_producer_id_block
  - /log_dir_event_notification

### **broker总体工作流程**

1. broker启动后在ZK中注册
2. 每个broker都有controller，谁先注册到ZK，谁说了算
3. 由选举出的Controller监听brokers节点变化
4. Controller决定Leader选举

    选举规则: 在ISR中存活为前提，按照AR(kafka分区中所有副本的统称,每个partition有一个leader)中排在前面的优先。例如ar[1,0,2]，isr[0,1,2]，就会按照1,0,2的顺序轮询

5. Controller将节点信息上传到ZK
6. 其他Controller从ZK同步相关信息

    发送信息后在副本间同步，以segment(1G)存储，后缀为.log，.index文件辅助索引

7. 假设Leader挂了，ZK中存的brokers中就会去除对应id
8. 其他Controller监听到该变化
9. 获取ISR
10. 选举新的Leader

## 生产经验

### **服役新节点**

修改broker.id后启动kafka即可，ZK不用启动因为连接到前三台的ZK集群即可

执行负载均衡

```bash
$ vim topics-to-move.json

# json
{
    "topics": [
        {"topic": "demo"}
    ],
    "version": 1
}

# 指定配置了要迁移的主题的json文件，指定主题迁移到哪些broker上，得到新的负载均衡的计划
$ bin/kafka-reassign-partitions.sh --bootstrap-server ipaddr:9092 --topics-to-move-json-file topics-to-move.json --broker-list "0,1,2,3" --generate

# 将该新计划的配置放到json文件中
$ vim increase-replication-factor.json

# json
{...}

# 执行计划
$ bin/kafka-reassign-partitions.sh --bootstrap-server ipaddr:9092 --reassignment-json-file increase-replication-factor.json --execute

# 验证计划
$ bin/kafka-reassign-partitions.sh --bootstrap-server ipaddr:9092 --reassignment-json-file increase-replication-factor.json --verify
```

### **退役旧节点**

类似服役新节点，要先配置好新的负载均衡

创建计划时把要退役的节点的broker.id去掉，其余命令一致

执行完成后将旧节点的kafka服务器停止即可

## kafka 副本

### **副本基本信息**

### **leader选举流程**

### **leader和follower故障处理细节**

### **分区副本分配**

### **生产经验——手动调整分区副本存储**

### **生产经验——Leader Partition 负载平衡**

### **生产经验——增加副本因子**

## 文件存储

### **文件存储机制**

### **文件清理策略**

## 高效读写数据
