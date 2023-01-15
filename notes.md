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