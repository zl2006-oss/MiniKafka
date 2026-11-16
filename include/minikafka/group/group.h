#ifndef MINIKAFKA_GROUP_GROUP_H_
#define MINIKAFKA_GROUP_GROUP_H_

// M6 消费组协调对外头文件汇总：静态分配（assignment.h）与位点协调器
// （coordinator.h）。本头不引入协议结构，Broker 负责响应编解码映射。
#include "minikafka/group/assignment.h"
#include "minikafka/group/coordinator.h"

#endif  // MINIKAFKA_GROUP_GROUP_H_
