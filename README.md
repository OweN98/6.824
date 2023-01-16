Progress:  
:heavy_check_mark::Lab 1, Lab 2A, Lab 2B basic agree

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

Reason: In `ticker()`, the routine should sleep `ElectionTimeout` at first, then judge if necessary to start election. I got in wrong order to judge at first. Stupid mistake. I should go back to primary school.

## Lab 2B:
### TestBasicAgree2B():
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
* ![2B_bug1.png](Pics%2F2B_bug1.png)  
    The first index should be 1, which has been told in Figure 2, why didn't I read it more clearly before.
    