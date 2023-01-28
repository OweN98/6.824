Progress:  
:heavy_check_mark::Lab 1, 2A, 2B, 2C

## Lab 2A:
![2A.png](Pics%2F2A.png)
### Bugs:
* TestInitialElection2A will get `warning: term changed even though there were no failures, term1: 4, term2:24`  
In test_test.sh, tester stops for a while, so no network failure right now
```
// sleep a bit to avoid racing with followers learning of the 
// election, then check that all peers agree on the term.
  time.Sleep(50 * time.Millisecond)
    term1 := cfg.checkTerms()
    if term1 < 1 {
        t.Fatalf("term is %v, but should be at least 1", term1)
  }

  // does the leader+term stay the same if there is no network failure?
  time.Sleep(2 * RaftElectionTimeout)
  term2 := cfg.checkTerms()
  if term1 != term2 {
     fmt.Printf("warning: term changed even though there were no failures, term1: %d, term2:%d\n", term1, term2)
  }
```
The docs in lab pages also says ***"for the leader to remain the leader if there are no failures"***, so the codes now can not do it.  
The step to maintain leader is HeartBeat, the null AppendMessage will make Follower reset ElectionTimeout.
So is the reason.

Solution: In `ticker()`, the routine should sleep `ElectionTimeout` at first, then judge the state(Follower/Candidate) to start election. I got in wrong order to judge at first. Stupid mistake. I should go back to primary school.

## Lab 2B:   

![PASSTest2B.png](Pics%2FPASSTest2B.png)  

Spend so much time on TestBackup2B :disappointed_relieved:
### TestBasicAgree2B:
![funcTestBasicAgree2B.png](Pics%2FfuncTestBasicAgree2B.png)  
![2BBasicAgree.png](Pics%2F2BBasicAgree.png)  
Flow:  
**The first index in log is "*1*", not '0', see detail in bugs**
1. Initilization
2. in for loops, keep sending command `100` to servers for 3 times.
- `nd, cmd := cfg.nCommitted(index)` counts servers that think the log entry at index is committed. `nd` is the count number of servers that committed the entry. `cmd` is the command of this index.
- `xindex := cfg.one(index*100, servers, false)` does a complete agreement. In a 10 seconds timeout for loop, it first pick out the leader right now and apply `Start(command)` to append new log entries to the leader. Then, if the leader exists, in a 2 seconds timeout loop, keep checking if other servers commit the new entries by using `nd, cmd1 := cfg.nCommitted(index)`. Of course servers will receive log entries included in periodic heartbeats.
  ![2BBasicAgreePrint0.png](Pics%2F2BBasicAgreePrint0.png)
- At last, the agreement will apply command `100` to the servers for 3 times.
  ![2BBasicAgreePrint1.png](Pics%2F2BBasicAgreePrint1.png)


#### Bugs:  

* ![2B_bug0.png](Pics%2F2B_bug0.png)  

    Only leader committed logs. Forget to commit logs in Followers.   
* The first index should be 1, which has been told in Figure 2, why didn't I read it more clearly before.
  ![2B_bug1.png](Pics%2F2B_bug1.png)

### TestRPCBytes2B:
each command is sent to each peer just once.
![TestRPCBytes2B.png](Pics%2FTestRPCBytes2B.png)
### TestFailAgree2B:
Test that a follower participates after disconnect and re-connect.  
  
![TestFailAgree2B.png](Pics%2FTestFailAgree2B.png)
![2BTestFailAgree.png](Pics%2F2BTestFailAgree.png)  

#### Bugs:
1. After one of the server disconnect from the network, the leader and other servers can't agree. The leader itself can not commit?  

![TestFailAgree2B_bug0.png](Pics%2FTestFailAgree2B_bug0.png)  
Solution: Forget to commit the log entries in Leader when entries are appended in `Start(command)`, so when the leader at last calculates count of the committed entries in the same index, the count is 1 less(not counting the leader).   
  
![TestFailAgree2B_bug0Solution.png](Pics%2FTestFailAgree2B_bug0Solution.png)

2. When the server re-connects to the network, the leader and other servers can't agree. The re-connected one will keep meeting ElectionTimeout and start election. The leader stops sending HeartBeat, the network crashed.  
Solution: when the server comes back, its term should be larger than existing servers(it always asks for election). So when the leader send HeartBeat to the coming back server, the fresher term will reply false. Then the leader deal with the reply and update term itself and be Follower to start election.  

![TestFailAgree2B_bug1Solution.png](Pics%2FTestFailAgree2B_bug1Solution.png)

3. As the picture above, the lastIndex fails to update which means that the logs are not appended successfully to the previous missing server.  
Solution: In `AppendEntries()`, if `args.PrevLogIndex > len(rf.logEntry)-1`, should not return immediately, or the new entries with older index will never be appended if the follower lacks older entries.  

 ![TestFailAgree2B_bug2Solution.png](Pics%2FTestFailAgree2B_bug2Solution.png)  

### TestFailNoAgree2B
Most of the servers failed, so all of the entries will be uncommited, thus never apply. But when servers come back, start new election and keep going on.  

![TestFailNoAgree2B.png](Pics%2FTestFailNoAgree2B.png)

### TestConcurrentStarts2B
When several commands are requested concurrently, the leader ensure that one command is processed at one time. And no miss due to concurrency.  

![TestConcurrentStarts2B.png](Pics%2FTestConcurrentStarts2B.png)

### TestRejoin2B
Start -> Add entry 101 to the leader in network-> disconnect leader -> add entries 102, 103, 104 to privately to the missing leader -> add entry 103 to the network -> disconnect the current leader -> connect the old leader -> add entry 104 to network -> connect the second disconnected leader -> add entry 105 to network  

![TestRejoin2B.png](Pics%2FTestRejoin2B.png)  

#### Bug:
1. Mistakenly set PrevlogIndex, so in `AppendEntries` RPC, the rules in 5.3 in paper which tells to find the latest two agreed log entry and delete the logs after that in Follower is not satisfied.
![TestRejoin2B_bug0.png](Pics%2FTestRejoin2B_bug0.png)

2. When now leader disconnects and old leader come back, the network started election, but keep election for a long time.
  
![TestRejoin2B_bug1.png](Pics%2FTestRejoin2B_bug1.png)

Reason: Variable conflict
### TestBackup2B  

![PassTestBackup2B .png](Pics%2FPassTestBackup2B%20.png)

| Server\Round |     1     |    2     |     3      |     4     |      5      |      6       |      7       |      8       |
|:------------:|:---------:|:--------:|:----------:|:---------:|:-----------:|:------------:|:------------:|:------------:|
|      0       | **[1]1*** | *[1]51** |    [1]x    |   [1]x    | **[50]51**  | **[50]101**  | **[50]101**  | **[50]102**  |
|      1       | **[1]1**  |    x     | **[2]51*** | *[2]101** |    [2]x     |     [2]x     | **[50]101**  | **[50]102**  |
|      2       | **[1]1**  |    x     | **[2]51**  | *[2]101*  |    [2]x     |     [2]x     | **[50]101**  | **[50]102**  |
|      3       | **[1]1**  |    x     | **[2]51**  |   [5]x    | **[50]51*** | **[50]101*** | **[50]101*** | **[50]102*** |
|      4       | **[1]1**  | *[1]51*  |   [25]x    |   [45]x   | **[50]51**  | **[50]101**  | **[50]101**  | **[50]102**  |  
This is the basic demo procedure of the process, the number in the table means the `CommitIndex`, the [x] means the xth `term`, * means the actual `leader` in the network. x means the server is `disconnected`. The *Italic* means `not committed`. The **bold** means `committed`.  
  
One of the importance point is between round 4 and 5, that when old leader S0 is back. It at first sends new log(same term 1 as old leader's term) to the servers S3 & S4. S3 & S4 both have larger team to let S1 update its term. And S3 is more up-to-date because it has logs with larger term, it will not add the log. S0 sends to S4 which has the same logs as itself, also will not add logs. So the electionTimeout is not reset. Thus start election. The S3 is the last winner due to the logs with larger term. But it will take terms to finish. Sometimes the test fails because the leader is not selected after many rounds election. So I set the electionTimeout to be as seperated as possible like `rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(300)+200)`. As the 5.2 in paper says.  
One more point is the update of `rf.nextIndex[]` when from round 4 - 5, the new leader S3 needs to send 50 logs in term 2 to S0 & S4. The original method is the decrease one by one, but it costs so many RPCs that the test fails early. The detail is in Bug 2.


#### Bugs:  
1. When brings back the servers which are partitioned at first and a later disconnected server, the leader should be the later disconnected server for the reason that is has more up-to-date logs. Some problems exist in `VoteRequest`  

   ![TestBackup2B_Bug0.png](Pics%2FTestBackup2B_Bug0.png)  

Solved. Forget to include "have voted" situation when `args.Term > rf.currentTerm`
![TestBackup2B_Bug0_A.png](Pics%2FTestBackup2B_Bug0_A.png)   


2. After bringing back the old leader, and select the up-to-date one as the leader, too many conflicting entries make the decrement of the `nextIndex[i]` to be very slow, so the test will fail.  

![TestBackup2B_bug3.png](Pics%2FTestBackup2B_bug3.png)

Solution: According to the paper, the follower can include the term of the conflicting entry and the first index it stores for that term. So it will reduce much time and pass the test.

## 2C
If 2B is done perfectly, 2C is very easy to complete, just to finish `persist()` and `readPersist()`  
![PASSTest2C.png](Pics%2FPASSTest2C.png)

  In `TestFigure8Unreliable2C` which is the toughest test in 2C, generates large number of new logs and at the same time make the network in chaos. In my debugging process, the term came into chaos. By reading the **Term confusion** from the students-guide-to-raft from the lab page, I knew that my network is not able to cope with old RPC replies when chaos, so when the leader receive replies, compare the current term with the original term sent in original RPC. If different, drop the reply and return. And this works.
  
## 2D

Main tasks:
1. Write Snapshot codes  
2. Rewrite all the variable about index and term. It's very annoying.

### TestSnapshotBasic2D  
Don't need to care about the InstallSnapshot RPC in the first test.
![PASSTestSnapshotBasic2D.png](Pics%2FPASSTestSnapshotBasic2D.png)

### TestSnapshotInstall2D  
Much workload on rewriting true index and term in every place.
![PASSTestSnapshotInstall2D.png](Pics%2FPASSTestSnapshotInstall2D.png)