#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "raft.h"
#include "util.h"
// void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);
// 这里是日志复制
void Raft::Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_status != Leader) {
    *isLeader = false;
    return;
  }
  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_logterm(m_currentTerm);
  newLogEntry.set_logindex(getNewCommandIndex());
  newLogEntry.set_command(command.asString());

  m_logs.push_back(newLogEntry);

  int lastLogIndex = getLastLogIndex();

  persist();

  for (int i = 0; i < m_peers.size(); i++) {
    if (i == m_me) {
      continue;
    }
    sendAppendEntries(i);
  }

  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums) {
  // 注意这里同步阻塞 先发送append然后去等待follower响应再去处理响应
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());
  if (!ok) {
    return ok;
  } /* term 决定一切：谁 term 高谁说了算 Leader 的所有行为必须基于当前合法的 term
  如果你发现你 term 落后了，你必须立刻放弃 Leader 身份，不再执行 Leader 行为
  所以我们需要上来就检查任期而不是检查当前的身份是否是leader */
  /* if (m_status != Leader) {
    return false;
  }
  if (m_status == Leader) {
  } */
  // 此时发现自己是leader了 处理回复
  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = reply->term();
    /* 注意这里为什么要处理选票呢 看起来明明是没有关系的呀 这是是日志追加啊
      按 Raft 协议：
    无论你是发送心跳，日志复制，还是请求投票，只要你发现自己的 term 落后了，你就得：
        更新 term；
        立刻降为 Follower；
        清空投票记录；
        持久化这三个状态（term、votedFor、logs）。
        */
    m_votedFor = -1;
    persist();
  } else if (reply->term() < m_currentTerm) {
    DPrintf("收到过时的请求");
    return ok;
  }
  if (reply->term() == m_currentTerm) {
    if (reply->success() == false) {
      m_nextIndex[server] = reply->updatenextindex();
    } else {
      *appendNums = *appendNums + 1;
      // 为了避免匹配索引遭到网络延迟影响而回调 因此要用max保持它单调递增
      m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries().size());
      m_nextIndex[server] = m_matchIndex[server] + 1;
      if (*appendNums >= (m_peers.size() / 2) + 1) {
        *appendNums = 0;
        leaderUpdateCommitIndex();
      }
    }
  }
  return ok;
}
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply) {
  // 这里主要实现以下框架
  // 上锁

  // 任期检查

  // 日志一致性检查

  // 日志追加

  // 更新提交索引

  // 返回响应
}
void Raft::leaderHearBeatTicker() {
  // 检查时间 设置睡眠时间 避免空转

  // 检查实际睡眠时间  查看是否超时

  // 根据是否超时也就是是否重置了定时器来决定 是否发起心跳
}
void Raft::doHeartBeat() {
  // 检查自己的身份

  // 日志一致性检查

  // 在快照之前 利用快照rpc恢复

  // 在快照之后 通过日志追加来补齐
}
void Raft::leaderSendSnapShot(int server) {
  //  检查身份

  // 封装请求和响应调用远程rpc

  // 处理响应
}

void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                           raftRpcProctoc::InstallSnapshotResponse *reply) {
  // 检查任期

  // 截断日志

  // 更新状态

  // 应用状态

  // 持久化存储
}

// 这里是日志提交及应用
void Raft::leaderUpdateCommitIndex() {
  // sendappendtries简化了流程 每次 AE RPC 收到多数成功响应就判断是否提交
  //  这里每次收到成功回复就遍历 matchIndex[] 计算最大可提交 index
  //  提交要求： 提交必须“过半复制 + 当前 term”
}
void Raft::applierTicker() {
  // 把 commitIndex 区间的新日志，apply 到状态机（即 push 给 KVServer）

  // 首先是获取新提交的日志

  // 然后通过push发送给我们的kvserver

  // 休眠一个apply提交时间
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
  // 从上次应用的加1然后到现在提交的commitindex 在封装成applymsg 在返回给
}
