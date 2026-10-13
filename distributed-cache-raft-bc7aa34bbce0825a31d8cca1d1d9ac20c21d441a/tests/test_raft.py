from dcache.raft import RaftNode

def test_raft_state():
    r = RaftNode("n1",["n2","n3"])
    r.become_candidate()
    assert r.current_term == 1
    r.become_leader()
    assert r.role == "leader"

def test_majority():
    r = RaftNode("n1",["n2","n3"])
    assert r.can_commit(2,3)
    assert not r.can_commit(1,3)
