/* 就是实现了clerk和raft集群的互动  主要就是putappend方法
这里统一实现了putappend根据具体的参数选择是哪一个方法put 或者append
但是注意这里只是请求 然后决定是否重试或者处理结果
 */
#include "clerk.h"
#include <string>
#include <vector>
#include "raftServerRpcUtil.h"
#include "util.h"

std::string Clerk::Get(std::string key) {
  m_requestId++;                  // 这是一个递增的请求id
  auto requestId = m_requestId;   // 复制当前的请求id
  int server = m_recentLeaderId;  // 复制领导节点为目标服务器节点
  raftKVRpcProctoc::GetArgs args;
  args.set_key(key);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);

  int retries = 0;
  const int maxRetries = 5;//设立最大次数为5


  while (retries < maxRetries) {
    raftKVRpcProctoc::GetReply reply;
    bool ok = m_servers[server]->Get(&args, &reply);
    //优化错误处理 日志增强 同时加入最大次数限制
    if (!ok ||
        reply.err() ==
            ErrWrongLeader) {  // 会一直重试，因为requestId没有改变，因此可能会因为RPC的丢失或者其他情况导致重试，kvserver层来保证不重复执行（线性一致性）
              retries++;
              server = (server + 1) % m_servers.size();
      continue;
    }
    if (reply.err() == ErrNoKey) {
      return "";
    }
    if (reply.err() == OK) {
      m_recentLeaderId = server;
      return reply.value();
    }
  }

  // 日志输出
  DPrintf(" 重试次数超限");
  return "";
}

void Clerk::PutAppend(std::string key, std::string value, std::string op) {
  // You will have to modify this function.
  m_requestId++;
  auto requestId = m_requestId;
  auto server = m_recentLeaderId;

  while (true) {
    raftKVRpcProctoc::PutAppendArgs args;
    args.set_key(key);
    args.set_value(value);
    args.set_op(op);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);

    raftKVRpcProctoc::PutAppendReply reply;
    bool ok = m_servers[server]->PutAppend(&args, &reply);
    
    if (!ok || reply.err() == ErrWrongLeader) {
      DPrintf("【Clerk::PutAppend】原以为的leader：{%d}请求失败，向新leader{%d}重试  ，操作：{%s}", server, server + 1,
              op.c_str());
      if (!ok) {
        DPrintf("重试原因 ，rpc失敗 ，");
      }
      if (reply.err() == ErrWrongLeader) {
        DPrintf("重試原因：非leader");
      }
      server = (server + 1) % m_servers.size();  // try the next server
      continue;
    }
    if (reply.err() == OK) {  // 什么时候reply errno为ok呢？？？
      m_recentLeaderId = server;
      return;
    }
  }
}

void Clerk::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }

void Clerk::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }
// 初始化客户端
void Clerk::Init(std::string configFileName) {
  // 获取所有raft节点ip、port ，并进行连接
  MprpcConfig config;
  config.LoadConfigFile(configFileName.c_str());
  std::vector<std::pair<std::string, short>> ipPortVt;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    std::string node = "node" + std::to_string(i);

    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty()) {
      break;
    }
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));  // 沒有atos方法，可以考慮自己实现
  }
  // 进行连接
  for (const auto& item : ipPortVt) {
    std::string ip = item.first;
    short port = item.second;
    // 2024-01-04 todo：bug fix
    auto* rpc = new raftServerRpcUtil(ip, port);
    m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
  }
}

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
