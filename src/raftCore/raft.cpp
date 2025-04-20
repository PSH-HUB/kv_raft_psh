#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"
// args 是leaders发送的请求参数   reply是follower 给leaders的响应  用于日志同步和心跳机制 所以这里主要是针对follower的
// 检查任期号（Term）是否一致。 检查日志一致性。 追加新的日志条目。更新提交索引（Commit Index）。设置回复状态。
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
  //  1.互斥锁 预处理
  std::lock_guard<std::mutex> locker(m_mtx);
  reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的
  //	不同的人收到AppendEntries的反应是不同的，要注意无论什么时候收到rpc请求和响应都要检查term
  //  2.检查任期
  if (args->term() < m_currentTerm) {  // 如果接收到的请求的任期小于当前自己的任期 应该直接拒绝
    reply->set_success(false);         // 设置false 表明拒绝请求
    reply->set_term(m_currentTerm);    // 把自己的任期告诉leader 让他更新
    reply->set_updatenextindex(-100);  // 论文中：让领导人可以及时更新自己
    DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
    return;  // 注意从过期的领导人收到消息不要重设定时器
  }
  DEFER { persist(); };                // 由于这个局部变量创建在锁之后，因此执行persist的时候应该也是拿到锁的.
  if (args->term() > m_currentTerm) {  // 如果大于自己的任期的话
    // 三变 ,防止遗漏，无论什么时候都是三变
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;  // 这里设置成-1有意义，如果突然宕机然后上线理论上是可以投票的
    // 这里可不返回，应该改成让改节点尝试接收日志
    // 如果是领导人和candidate突然转到Follower好像也不用其他操作
    // 如果本来就是Follower，那么其term变化，
    // 相当于“不言自明”的换了追随的对象，因为原来的leader的term更小，是不会再接收其消息了
  }
  // 下面是断言确认两者任期一致  否则就出发断言失败
  myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
  // 如果发生网络分区，那么candidate可能会收到同一个term的leader的消息，要转变为Follower，为了和上面，因此直接写
  m_status = Follower;  // 这里是有必要的，因为如果candidate收到同一个term的leader的AE，需要变成follower
  // term相等
  m_lastResetElectionTime = std::chrono::system_clock::now();
  // 不能无脑的从prevlogIndex开始阶段日志，因为rpc可能会延迟，导致发过来的log是很久之前的

  //	那么就比较日志，日志有3种情况
  // 3.这里就是一致性检查部分  检查日志索引是否合法 就是是否在我们合理的范围之内
  // 上面是判断任期 下面就开始比较日志了
  if (args->prevlogindex() >
      getLastLogIndex()) {  // 请求的上一个日志索引要大于当前节点的日志索引  跟随者&旧领导者缺少新领导者拥有的日志条目
                            // 拒绝请求自己无法同步日志 按理说它的上一步应该就是我当前的最后一个
    reply->set_success(false);                          // 拒绝请求
    reply->set_term(m_currentTerm);                     // 回复里仍然要带上任期号 确保leader收到最新的消息
    reply->set_updatenextindex(getLastLogIndex() + 1);  // 告诉leader下次从那个日志索引开始发送
    return;
  } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {  // 请求的上一个日志索引小于当前节点的快照包含的索引。
                                                                   // 跟随者&旧领导者拥有新领导者缺少的日志条目
    // 说明请求中的日志条目已经包含在快照中，无法直接追加到当前节点的日志中。
    reply->set_success(false);       // 拒绝
    reply->set_term(m_currentTerm);  // 更新任期
    reply->set_updatenextindex(
        m_lastSnapshotIncludeIndex +
        1);  // todo 如果想直接弄到最新好像不对，因为是从后慢慢往前匹配的，这里不匹配说明后面的都不匹配
  }
  // 日志冲突意味着 Follower 的日志在 prevLogIndex 之后的部分是错误的，需要截断（删除）错误的部分，并从 prevLogIndex
  // 重新接收 Leader 的日志
  // 注意：这里目前当args.PrevLogIndex == rf.lastSnapshotIncludeIndex与不等的时候要分开考虑，可以看看能不能优化这块
  // 4.日志的一致性检查
  // 先是匹配的情况 追加日志条目 异常检查  更新提交索引 设置回复状态
  if (matchLog(args->prevlogindex(), args->prevlogterm())) {  //
    for (int i = 0; i < args->entries_size(); i++) {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex()) {
        // 如果新的日志条目的索引大于当前节点的日志索引，直接追加。
        m_logs.push_back(log);
      } else {
        // 如果新的日志条目的索引小于或等于当前节点的日志索引，检查任期号是否一致。如果不一致，更新当前节点的日志条目。
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
          // 相同位置的log ，其logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了！
          myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                 " {%d:%d}却不同！！\n",
                                 m_me, log.logindex(), log.logterm(), m_me,
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                 log.command()));
        }
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
          // 不匹配就更新
          m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
        }
      }
    }
    // 因为可能会收到过期的log！！！ 因此这里是大于等于
    // 日志追加成功之后的后续  更新提交索引 设置回复状态
    myAssert(
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
    // 如果领导者提供的已提交日志索引大于当前节点的已提交日志索引，更新当前节点的已提交日志索引。
    if (args->leadercommit() > m_commitIndex) {
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
      // std::min确保m_commitIndex不会超过当前节点的日志索引范围。
    }
    // 检查提交索引范围    领导会一次发送完所有的日志
    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);

    //        DPrintf("[func-AppendEntries-rf{%v}] 接收了来自节点{%v}的log，当前lastLogIndex{%v}，返回值：{%v}\n",
    //        rf.me,
    //                args.LeaderId, rf.getLastLogIndex(), reply)

    return;
    // 日志不匹配状态 更新索引 找到第一个不匹配的日志条目
  } else {
    // PrevLogIndex 长度合适，但是不匹配，因此往前寻找 矛盾的term的第一个元素
    // 先将updatenextindex设置为args->prevlogindex()，是为了确保即使循环没有找到任何不匹配的日志条目，updatenextindex也有一个合理的初始值。
    reply->set_updatenextindex(args->prevlogindex());
    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
      // 如果发现某个日志条目的任期号与 args->prevlogterm() 不匹配，说明找到了第一个不匹配的日志条目。
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    return;
  }
}

void Raft::applierTicker() {
  while (true) {
    m_mtx.lock();
    if (m_status == Leader) {
      DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
              m_commitIndex);
    }
    auto applyMsgs = getApplyLogs();
    m_mtx.unlock();
    // 使用匿名函数是因为传递管道的时候不用拿锁
    //  todo:好像必须拿锁，因为不拿锁的话如果调用多次applyLog函数，可能会导致应用的顺序不一样
    if (!applyMsgs.empty()) {
      DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver報告的applyMsgs長度爲：{%d}", m_me, applyMsgs.size());
    }
    for (auto& message : applyMsgs) {
      applyChan->Push(message);  // 通过 applyChan 管道推送到主逻辑（KVServer）
    }
    // usleep(1000 * ApplyInterval);
    sleepNMilliseconds(ApplyInterval);
  }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  return true;
  //// Your code here (2D).
  // rf.mu.Lock()
  // defer rf.mu.Unlock()
  // DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex {%v} to check
  // whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)
  //// outdated snapshot
  // if lastIncludedIndex <= rf.commitIndex {
  //	return false
  // }
  //
  // lastLogIndex, _ := rf.getLastLogIndexAndTerm()
  // if lastIncludedIndex > lastLogIndex {
  //	rf.logs = make([]LogEntry, 0)
  // } else {
  //	rf.logs = rf.logs[rf.getSlicesIndexFromLogIndex(lastIncludedIndex)+1:]
  // }
  //// update dummy entry with lastIncludedTerm and lastIncludedIndex
  // rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
  //
  // rf.persister.Save(rf.persistData(), snapshot)
  // return true
}
// 选举过程 在选举定时器超时时使用 目的时让当前节点发起一次选举 成为new  leader
void Raft::doElection() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {  // 这个没什么好说的
  }

  if (m_status != Leader) {
    DPrintf("[ ticker-func-rf(%d)] 选举定时器到期且不是leader，开始选举 \n", m_me);
    // 当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡主
    // 重竞选超时，term也会增加的
    // 开始选举 身份为candidate
    m_status = Candidate;
    /// 开始新一轮的选举
    m_currentTerm += 1;  // 开始选举 先增加任期
    m_votedFor = m_me;   // 即是自己给自己投，也避免投出去给到了其他的同任期节点
    persist();           // 对以上数据进行持久化  保证崩溃恢复时不会丢失关键状态，避免出现 选举混乱 或 任期回退
    std::shared_ptr<int> votedNum =
        std::make_shared<int>(1);  // 使用 make_shared 函数初始化 !! 避免了内存分配失败 将构建对象和内存分配一起进行
    //	重新设置定时器
    m_lastResetElectionTime = std::chrono::system_clock::now();
    //	发布RequestVote RPC
    //  设置请求参数和响应参数创建工作线程调用 发送给其他节点
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;  // 这里的初始化是为了表示当前节点日志为空 这样便于函数内部处理叭
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  // 获取最后一个log的term和下标
      /* 1.Candidate 的 当前任期 (m_currentTerm) 大于 Follower 的任期。
         2.Candidate 的 日志比 Follower 的更新：
             Candidate.lastLogTerm > Follower.lastLogTerm
             lastLogTerm 相等  ： Candidate.lastLogIndex >= Follower.lastLogIndex */

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

      // 使用匿名函数执行避免其拿到锁
      // this：Raft 实例（绑定成员函数）i：目标Follower节点ID   requestVoteArgs：封装了投票请求参数
      // requestVoteReply：存储投票响应 votedNum：记录当前获得的投票数
      std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                    votedNum);  // 创建新线程并执行b函数，并传递参数
      t.detach();
    }
  }
}

void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
    auto appendNums = std::make_shared<int>(1);  // 正确返回的节点的数量

    // 对Follower（除了自己外的所有节点发送AE）
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
      // 日志压缩加入后要判断是发送快照还是发送AE
      // 就是要发送给follower的索引小于快照的最后一个索引
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        std::thread t(&Raft::leaderSendSnapShot, this, i);  // 创建新线程并执行b函数，并传递参数
        t.detach();
        // 处理完当前节点 然后continue处理下一个节点
        continue;
      }
      // 处理完发送快照的情况 处理发送AppendEntry
      // 构造发送值
      int preLogIndex = -1;
      int PrevLogTerm = -1;
      getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
      std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
          std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      appendEntriesArgs->set_term(m_currentTerm);
      appendEntriesArgs->set_leaderid(m_me);
      appendEntriesArgs->set_prevlogindex(preLogIndex);
      appendEntriesArgs->set_prevlogterm(PrevLogTerm);
      appendEntriesArgs->clear_entries();
      appendEntriesArgs->set_leadercommit(m_commitIndex);
      // 是从快照之后追加  还是索引之后的追加
      if (preLogIndex != m_lastSnapshotIncludeIndex) {
        // getSlicesIndexFromLogIndex将日志索引转换成 m_logs 中的数组索引
        for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
          // 添加了新的日志条目 并且添加指向日志条目的指针
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          // 将日志数据赋值添加给新的条目
          *sendEntryPtr = m_logs[j];  //=是可以点进去的，可以点进去看下protobuf如何重写这个的
        }
      } else {
        // 是快照的最后一条日志 所以从快照往后发就行
        for (const auto& item : m_logs) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = item;  //=是可以点进去的，可以点进去看下protobuf如何重写这个的
        }
      }
      int lastLogIndex = getLastLogIndex();
      // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
      // prevLogIndex 是 Leader 和 Follower 上次同步成功的日志的索引，即 Follower 上次接收到的日志的索引。
      // lastLogIndex 是 Leader 当前日志的最后一条日志的索引，表示 Leader 日志的最新状态。
      // entries_size() 是本次要发送的日志的数量
      myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%·d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
      // 构造返回值
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
          std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);

      std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply,
                    appendNums);  // 创建新线程并执行b函数，并传递参数
      t.detach();
    }
    m_lastResetHearBeatTime = std::chrono::system_clock::now();  // leader发送心跳，重置心跳时间
  }
}
// 决定是否发起选举
void Raft::electionTimeOutTicker() {
  // Check if a Leader election should be started.
  while (true) {
    while (m_status == Leader) {  // 这段代码的作用是 在 Leader 状态下，每隔 HeartBeatTimeout
                                  // 休眠一次，但它本身并没有发送心跳。因为它是leader leader并不参与选举
      // 定时时间没有严谨设置，因为HeartBeatTimeout比选举超时一般小一个数量级，因此就设置为HeartBeatTimeout了
      std::this_thread::sleep_for(std::chrono::microseconds(HeartBeatTimeout));
      // 注意这里heart是int的 但是sleepfor需要chrono类型的 又是以毫秒为单位那就是microseconds
      //  usleep(hHeartBeatTimeout);
    }
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};  // 代表剩余超时时间
    std::chrono::system_clock::time_point wakeTime{};                                       // 记录当前的时间
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      wakeTime = std::chrono::system_clock::now();
      // 剩余超时时间 = 随机超时时间 + 上次重置时间 - 当前时间
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
      m_mtx.unlock();
    }
    if (suitableSleepTime.count() > 0) {
      std::this_thread::sleep_for(suitableSleepTime);
    } else {
      doElection();
      continue;
    }
    // waketime两种时钟不同
    if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
      continue;
    }

    doElection();
    /* 它的核心功能是 检查 suitableSleepTime 是否大于 1 毫秒，如果是，则：记录当前时间 start
    调用 usleep() 让当前线程睡眠 suitableSleepTime
    记录睡眠结束时间 end
    计算实际睡眠时间 duration 并打印出来 */
    /* if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                << std::endl;
      std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                << std::endl;
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
      // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
      continue;
    } */
  }
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
  std::vector<ApplyMsg> applyMsgs;
  myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));

  while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;
    myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
    ApplyMsg applyMsg;
    applyMsg.CommandValid = true;
    applyMsg.SnapshotValid = false;
    applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
    applyMsg.CommandIndex = m_lastApplied;
    applyMsgs.emplace_back(applyMsg);
    //        DPrintf("[	applyLog func-rf{%v}	] apply Log,logIndex:%v  ，logTerm：{%v},command：{%v}\n",
    //        rf.me, rf.lastApplied, rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogTerm,
    //        rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].Command)
  }
  return applyMsgs;
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex() {
  //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}

// getPrevLogInfo
// leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
  // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    // 要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// GetState return currentTerm and whether this server
// believes it is the Leader.
void Raft::GetState(int* term, bool* isLeader) {
  m_mtx.lock();
  DEFER {
    // todo 暂时不清楚会不会导致死锁
    m_mtx.unlock();
  };

  // Your code here (2A).
  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}
// 这里主要是对应前面的
void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    // 如果leader任期小于自己的 给他自己的任期 结果返回
    // 注意这里 触发选举的条件是选举超时 而不是收到过时任期
    return;
  }
  if (args->term() > m_currentTerm) {
    // 后面两种情况都要接收日志
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
  }
  // 这里重复了   m_status = Follower;
  m_lastResetElectionTime = std::chrono::system_clock::now();  // 这里无论任期大小 只要两者交互了那就应该重置选举超时
  // outdated snapshot
  if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
    // 这里是 快照包含的最后索引要小于我的快照最后索引 所以没必要更新快照了
    return;
  }
  // 截断日志，修改commitIndex和lastApplied
  // 截断日志包括：日志长了，截断一部分，日志短了，全部清空，其实两个是一种情况
  // 但是由于现在getSlicesIndexFromLogIndex的实现，不能传入不存在logIndex，否则会panic
  auto lastLogIndex = getLastLogIndex();

  if (lastLogIndex > args->lastsnapshotincludeindex()) {
    // 当前日志大于快照的最后索引 那就删除掉日志中已经包含的条目 以便于接收新快照
    // 这些日志条目比 Leader 的快照包含的日志条目要“新”，所以这些日志条目应该保留，其他部分则应该被删除。
    m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
  } else {  // 如果小于的话 那就清除日志即可
    m_logs.clear();
  }
  m_commitIndex = std::max(m_commitIndex,
                           args->lastsnapshotincludeindex());  // 确保 m_commitIndex 不小于 Leader 快照中的最后日志索引
  m_lastApplied = std::max(m_lastApplied,
                           args->lastsnapshotincludeindex());  // 确保 m_lastApplied 不小于 Leader 快照中的最后日志索引
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);
  ApplyMsg msg;  // 用于封装将要被应用的快照消息
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();

  std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 创建新线程并执行b函数，并传递参数
  t.detach();
  // 将 Raft 实例的当前状态数据（通过 persistData() 获取）以及从领导节点接收到的快照数据（通过 args->data()
  // 获取）持久化到存储中。这是为了确保在系统崩溃或重启后，可以恢复到一致的状态。
  m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

// 类似于选举超时 负责查看是否发送心跳 就跟之前查看是否发起选举一样
void Raft::leaderHearBeatTicker() {
  while (true) {
    // 不是leader的话就没有必要进行后续操作，况且还要拿锁，很影响性能，目前是睡眠，后面再优化优化
    while (m_status != Leader) {
      usleep(1000 * HeartBeatTimeout);
      // std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
    }
    static std::atomic<int32_t> atomicCount = 0;
    // 表示当前进程需要睡眠的时间 基于heartBeatTimeout  m_lastRestHeartBeatTime
    // 目的：用于动态调整睡眠时间 避免线程频繁检查导致cpu空转
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      wakeTime = std::chrono::system_clock::now();
      suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                << std::endl;
      // 获取当前时间点
      auto start = std::chrono::system_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);

      // 获取函数运行结束后的时间点
      auto end = std::chrono::system_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                << " 毫秒\033[0m" << std::endl;
      ++atomicCount;
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0) {
      // 睡眠的这段时间有重置定时器，没有超时，再次睡眠
      continue;
    }
    // DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了\n", m_me);
    doHeartBeat();
  }
}

void Raft::leaderSendSnapShot(int server) {
  // 资源上锁 但是感觉这里可以用智能指针
  m_mtx.lock();
  raftRpcProctoc::InstallSnapshotRequest args;  // 封装要发送的快照消息 里面包含了身份 任期 最后的快照索引以及任期
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());  // 这里是实际的数据 要从已经持久化的数据里面读取

  raftRpcProctoc::InstallSnapshotResponse reply;              // 封装了要接受的响应 里面是follower的回复
  m_mtx.unlock();                                             // 加锁防止多个线程同时修改args
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);  // 这里只表示rpc调用是否成功 主要是让follower安装快照
  m_mtx.lock();                                               // 涉及到状态的修改都需要加锁
  DEFER { m_mtx.unlock(); };                                  // 使用 RAII 方式，确保函数退出时自动释放锁
  if (!ok) {
    return;
  }
  if (m_status != Leader || m_currentTerm != args.term()) {
    return;  // 中间释放过锁，可能状态已经改变了
  }
  //	无论什么时候都要判断term
  if (reply.term() > m_currentTerm) {
    // 三变
    m_currentTerm = reply.term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
}

void Raft::leaderUpdateCommitIndex() {
  m_commitIndex = m_lastSnapshotIncludeIndex;
  // for index := rf.commitIndex+1;index < len(rf.log);index++ {
  // for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
  for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--) {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        sum += 1;
        continue;
      }
      if (m_matchIndex[i] >= index) {
        sum += 1;
      }
    }

    //        !!!只有当前term有新提交的，才会更新commitIndex！！！！
    // log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSSPointIndex, index, rf.commitIndex,
    // rf.getLastIndex())
    if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
      m_commitIndex = index;
      break;
    }
  }
  //    DPrintf("[func-leaderUpdateCommitIndex()-rf{%v}] Leader %d(term%d) commitIndex
  //    %d",rf.me,rf.me,rf.currentTerm,rf.commitIndex)
}

// 进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex	，而且小于等于rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
  // if logIndex == rf.lastSnapshotIncludeIndex {
  // 	return logTerm == rf.lastSnapshotIncludeTerm
  // } else {
  // 	return logTerm == rf.logs[rf.getSlicesIndexFromLogIndex(logIndex)].LogTerm
  // }
}

void Raft::persist() {
  // Your code here (2C).
  auto data = persistData();
  m_persister->SaveRaftState(data);
  // fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm,
  // rf.votedFor, rf.logs) fmt.Printf("%v\n", string(data))
}
// 用于响应sendRequestVote 是否投票让其成为领导 注意此时当前节点应该是follower
void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);

  // Your code here (2A, 2B).
  DEFER {
    // 应该先持久化，再撤销lock
    persist();
  };
  // 对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
  //  reason: 出现网络分区，该竞选者已经OutOfDate(过时）
  // 分别回复 自己的任期
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    //  Expire = 2  投票（消息、竞选者）过期
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    return;
  }
  // fig2:右下角，如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
  if (args->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;

    //	重置定时器：收到leader的ae，开始选举，透出票
    // 这时候更新了term之后，votedFor也要置为-1
  }
  myAssert(args->term() == m_currentTerm,
           format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));
  //	现在节点任期都是相同的(任期小的也已经更新到新的args的term了)，还需要检查log的term和index是不是匹配的了

  int lastLogTerm = getLastLogTerm();
  // 只有没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会授票
  if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
    // args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
    // 下面的if 主要是用来输出错误信息的
    if (args->lastlogterm() < lastLogTerm) {
      // 日志的任期比当前节点的日志任期小，拒绝投票
      // 可以打印调试信息，标明拒绝投票的原因：日志任期过旧
    } else {
      // 如果任期相同，但候选者日志索引小于当前节点的日志索引
      // 可以打印调试信息，标明拒绝投票的原因：日志索引过旧
    }
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  }
  // 处理已经投票过的情况 防止一个节点在一个任期内进行多次投票
  // -1表示没有投票  这里如果要投票的话 votedfor会指向那个candidate的id的
  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  } else {
    m_votedFor = args->candidateid();
    // 认为必须要在投出票的时候才重置定时器，
    m_lastResetElectionTime = now();
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);

    return;
  }
}

bool Raft::UpToDate(int index, int term) {
  // lastEntry := rf.log[len(rf.log)-1]

  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  } else {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}
/**
 *
 * @return 最新的log的logindex，即log的逻辑index。区别于log在m_logs中的物理index
 * 可见：getLastLogIndexAndTerm()
 */
int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

/**
 *
 * @param logIndex log的逻辑index。注意区别于m_logs的物理index
 * @return
 */
int Raft::getLogTermFromLogIndex(int logIndex) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex) {
    return m_lastSnapshotIncludeTerm;
  } else {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// 找到index对应的真实下标位置！！！
// 限制，输入的logIndex必须保存在当前的logs里面（不包含snapshot）
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}
// 为了争取选票成为leader  当前节点一定是candidate
// 这里的 server 是目标节点服务器更准确说是网络代理 args是请求参数 reply是回复的结果 votednum 是投票数
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
  // 这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
  // ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  // todo
  // 获取当前时间
  auto start = now();
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 開始", m_me, m_currentTerm, getLastLogIndex());
  // 仅仅表示 RPC 调用没有失败，而不是投票一定成功。
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 完畢，耗時:{%d} ms", m_me, m_currentTerm,
          getLastLogIndex(), now() - start);

  if (!ok) {
    return ok;  // 不知道为什么不加这个的话如果服务器宕机会出现问题的，通不过2B  todo
  }
  // 这里是发送出去了，但是不能保证他一定到达
  //  对回应进行处理，要记得无论什么时候收到回复就要检查term
  std::lock_guard<std::mutex> lg(m_mtx);
  // 回复的term 比自己的大 说明自己落后了 那就变为follower
  if (reply->term() > m_currentTerm) {
    m_status = Follower;  // 三变：身份，term，和投票
    m_currentTerm = reply->term();
    // 重置投票了
    m_votedFor = -1;
    // 持久化存储
    persist();
    // 这里返回表示RPC 顺利发送并返回
    return true;
  } else if (reply->term() < m_currentTerm) {
    // 这里如果还小于的话 那就说明过时了 因为正常情况下 follower收到消息会更新自己的任期
    // 就保证了大部分的节点会更新自己的任期从而进行投票
    return true;
  }
  myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

  // 这里如果拒绝投票了 返回true 注意这里的意义是RPC是否执行成功 而不是是否成功拿到选票
  if (!reply->votegranted()) {
    return true;
  }
  // 这里是节点同意投票了 reply->votegranted() 所以选票加1
  //  这里表示的是收到的投票数 但是不要忘了 candidate会给自己投一票
  *votedNum = *votedNum + 1;
  // 选票数大于大多数节点 也就是一半以上的节点
  if (*votedNum >= m_peers.size() / 2 + 1) {
    // 变成leader  重置投票数
    *votedNum = 0;
    if (m_status == Leader) {
      // 如果已经是leader了，那么是就是了，不会进行下一步处理了k
      // 如果一个节点两次成为leader 一般是出现了网络问题导致再次选举成为leader 这不符合协议
      myAssert(false,
               format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
    }
    // 第一次变成leader，初始化状态和nextIndex、matchIndex
    m_status = Leader;

    DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm,
            getLastLogIndex());

    int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++) {
      // m_nextIndex[i]：表示领导者日志中，每个节点下一个需要复制的日志条目的索引。
      m_nextIndex[i] = lastLogIndex + 1;  // 有效下标从1开始，因此要+1
      // m_matchIndex[i]：表示每个节点已经复制到的日志条目的索引。
      m_matchIndex[i] = 0;  // 每换一个领导都是从0开始，见fig2
    }
    std::thread t(&Raft::doHeartBeat, this);  // 马上向其他节点宣告自己就是leader
    t.detach();

    persist();
  }
  return true;
}
// 负责发送日志的RPC，在发送完RPC后还需要负责接收并处理对端发送回来的响应。
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums) {
  // 这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
  //  如果网络不通的话肯定是没有返回的，不用一直重试
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc開始 ， args->entries_size():{%d}", m_me,
          server, args->entries_size());
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  if (!ok) {
    // rpc调用失败
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失敗", m_me, server);
    return ok;
  }
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
  if (reply->appstate() == Disconnected) {
    // 调用成功 但是follower因为网络问题或者其他原因未能处理请求 我们只要保证大多数节点可用即可
    return ok;
  }
  std::lock_guard<std::mutex> lg1(m_mtx);

  // 对reply进行处理 检查返回的term 维持日志的一致性 对于rpc通信，无论什么时候都要检查term
  if (reply->term() > m_currentTerm) {
    // 身份过期 降为follower    更新term 和 投票
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();  // 这里应该持久化 保持最新的内容
    return ok;
  } else if (reply->term() < m_currentTerm) {
    DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
            m_me, m_currentTerm);
    // 这里如果小于的话 说明follower过时了 应该忽略 正常情况下更新之后两者的term应该是一样的
    return ok;
  }

  if (m_status != Leader) {
    // 如果不是leader，那么就不要对返回的情况进行处理了
    return ok;
  }
  // term相等

  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
  if (!reply->success()) {  // 这里如果成功了 准确的说是成功调用了远程Rpc并收到了回复 错误的话 那就是其他问题
    // 日志不匹配，正常来说就是index要往前-1，既然能到这里，第一个日志（idnex =
    //  1）发送后肯定是匹配的，因此不用考虑变成负数 因为真正的环境不会知道是服务器宕机还是发生网络分区了
    if (reply->updatenextindex() != -100) {  // -100表示特殊标记 用于优化leader的回退逻辑
      DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
              server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex();  // 失败是不更新matchIndex的
    }
  } else {
    // follower 已成功复制日志，Leader 需要更新 matchIndex 和 nextIndex，并可能提交日志。
    *appendNums = *appendNums + 1;  // 这里记录的是成功的节点数
    DPrintf("---------------------------tmp------------------------- 節點{%d}返回true,當前*appendNums{%d}", server,
            *appendNums);
    // 计算matchindex matchindex 取当前日志的最大值 nextindex 取matchindex + 1
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;
    int lastLogIndex = getLastLogIndex();

    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                    m_logs.size(), server, lastLogIndex));
    // 超过半数节点成功复制日志  则可以提交日志
    if (*appendNums >= 1 + m_peers.size() / 2) {
      // 可以commit了
      // 两种方法保证幂等性，1.赋值为0 	2.上面≥改为==
      // 注意此时要复位 因为已经超过半数的节点复制了日志 所以日志可以提交
      *appendNums = 0;
      // leader只有在当前term有日志提交的时候才更新commitIndex，因为raft无法保证之前term的Index是否提交
      // 只有当前term有日志提交，之前term的log才可以被提交，只有这样才能保证“领导人完备性{当选领导人的节点拥有之前被提交的所有log，当然也可能有一些没有被提交的}”
      if (args->entries_size() > 0) {
        DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
      }
      if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
        DPrintf(
            "---------------------------tmp------------------------- 當前term有log成功提交，更新leader的m_commitIndex "
            "from{%d} to{%d}",
            m_commitIndex, args->prevlogindex() + args->entries_size());
        // 保证提交索引单调递增，不会回退。
        m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
      }
      myAssert(m_commitIndex <= lastLogIndex,
               format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                      m_commitIndex));
    }
  }
  return ok;
}

void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
  AppendEntries1(request, response);
  done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
  InstallSnapshot(request, response);

  done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
  RequestVote(request, response);
  done->Run();
}

void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
  std::lock_guard<std::mutex> lg1(m_mtx);
  if (m_status != Leader) {
    DPrintf("[func-Start-rf{%d}]  is not leader");
    *newLogIndex = -1;
    *newLogTerm = -1;
    *isLeader = false;
    return;
  }

  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_command(command.asString());
  newLogEntry.set_logterm(m_currentTerm);
  newLogEntry.set_logindex(getNewCommandIndex());
  m_logs.emplace_back(newLogEntry);

  int lastLogIndex = getLastLogIndex();

  // leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发
  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
  // rf.timer.Reset(10) //接收到命令后马上给follower发送,改成这样不知为何会出现问题，待修正 todo
  persist();
  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}

void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
  m_peers = peers;          // 与其他节点沟通的rpc类
  m_persister = persister;  // 持久化类
  m_me = me;                // 标记自己 该节点的唯一标识符  用来区分其余的节点
  // Your initialization code here (2A, 2B, 2C).
  m_mtx.lock();

  // applier
  this->applyChan = applyCh;  // 与kv-server沟通
  //    rf.ApplyMsgQueue = make(chan ApplyMsg)
  m_currentTerm = 0;    // 当前任期
  m_status = Follower;  // 大家一开始都初始化为follower
  m_commitIndex = 0;    // 初始化提交的日志索引
  m_lastApplied = 0;    // 初始化提交到状态机的日志
  m_logs.clear();       // 开始日志清空
  for (int i = 0; i < m_peers.size(); i++) {
    m_matchIndex.push_back(0);  // 用于记录当前已复制的某个节点i的日志的最高索引
    m_nextIndex.push_back(0);   // 用于记录leader的下一次给某个节点i发送日志的索引
  }
  m_votedFor = -1;  // 投票的初始状态

  m_lastSnapshotIncludeIndex = 0;                              // 快照的最后索引
  m_lastSnapshotIncludeTerm = 0;                               // 快照的最后任期
  m_lastResetElectionTime = std::chrono::system_clock::now();  // 记录选举超时的时间
  m_lastResetHearBeatTime = std::chrono::system_clock::now();  // 记录心跳超时的时间

  // initialize from state persisted before a crash
  readPersist(m_persister->ReadRaftState());  // 崩溃后读取持久化存储的状态
  if (m_lastSnapshotIncludeIndex > 0) {
    m_lastApplied = m_lastSnapshotIncludeIndex;
    // rf.commitIndex = rf.lastSnapshotIncludeIndex   todo ：崩溃恢复为何不能读取commitIndex
    /* 如果 m_lastSnapshotIncludeIndex > 0，说明已经安装过快照：
    需要把 m_lastApplied 设为 m_lastSnapshotIncludeIndex，因为快照已经应用到状态机。
    但 不能直接设置 m_commitIndex 为 m_lastSnapshotIncludeIndex，否则可能导致错误。
    1. 避免不一致问题
    commitIndex 需要确保：Leader 已经复制了相应的日志条目。大多数节点已经提交了该日志。但崩溃恢复时，节点可能还未与
    Leader 重新同步，如果直接把 commitIndex 设为
    lastSnapshotIncludeIndex，会导致Follower认为日志已提交，可能造成状态不一致。
    2. 需要等 Leader 重新确认 commitIndex
    Follower 需要等待 Leader 通过心跳或日志同步告知最新的 commitIndex，不能自己盲目修改。Leader 在 AppendEntries RPC
    里会同步 commitIndex，Follower 只有收到 Leader 的 commitIndex 更新后，才能安全地提交日志。*/
  }

  DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  m_mtx.unlock();  // 完成初始化之后解锁 以便于其他线程或者协程可以访问共享数据

  m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

  // start ticker fiber to start elections
  // 启动三个循环定时器
  // todo:原来是启动了三个线程，现在是直接使用了协程，三个函数中leaderHearBeatTicker
  // electionTimeOutTicker执行时间是恒定的，applierTicker时间受到数据库响应延迟和两次apply之间请求数量的影响，这个随着数据量增多可能不太合理，最好其还是启用一个线程。
  m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });  // leader发送心跳信号 保持领导身份
  m_ioManager->scheduler(
      [this]() -> void { this->electionTimeOutTicker(); });  // 超时选举           若未收到心跳 触发选举

  std::thread t3(&Raft::applierTicker, this);  // 将日志应用到状态机
  t3.detach();

  // std::thread t(&Raft::leaderHearBeatTicker, this);
  // t.detach();
  //
  // std::thread t2(&Raft::electionTimeOutTicker, this);
  // t2.detach();
  //
  // std::thread t3(&Raft::applierTicker, this);
  // t3.detach();
}

std::string Raft::persistData() {
  BoostPersistRaftNode boostPersistRaftNode;
  boostPersistRaftNode.m_currentTerm = m_currentTerm;
  boostPersistRaftNode.m_votedFor = m_votedFor;
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
  for (auto& item : m_logs) {
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << boostPersistRaftNode;
  return ss.str();
}

void Raft::readPersist(std::string data) {
  if (data.empty()) {
    return;
  }
  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  // read class state from archive
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
  m_logs.clear();
  for (auto& item : boostPersistRaftNode.m_logs) {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}

void Raft::Snapshot(int index, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex();  // 为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

  // 制造完此快照后剩余的所有日志
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  // todo :这种写法有点笨，待改进，而且有内存泄漏的风险
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    // 注意有=，因为要拿到最后一个日志
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  // rf.lastApplied = index //lastApplied 和 commit应不应该改变呢？？？ 为什么  不应该改变吧
  m_persister->Save(persistData(), snapshot);

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}