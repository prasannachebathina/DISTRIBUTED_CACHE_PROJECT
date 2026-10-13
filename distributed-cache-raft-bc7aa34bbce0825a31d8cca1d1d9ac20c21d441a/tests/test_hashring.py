from dcache.hashring import ConsistentHashRing

def test_route():
    r = ConsistentHashRing(20)
    for n in ["n1","n2","n3"]:
        r.add_node(n)
    assert r.get_node("student1") in {"n1","n2","n3"}

def test_remove():
    r = ConsistentHashRing(20)
    r.add_node("n1")
    r.add_node("n2")
    r.remove_node("n1")
    assert r.get_node("x") == "n2"
