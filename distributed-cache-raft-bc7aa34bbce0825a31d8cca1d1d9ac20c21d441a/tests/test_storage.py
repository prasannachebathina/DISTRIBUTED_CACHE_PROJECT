from dcache.storage import LSMStorage

def test_storage(tmp_path):
    s = LSMStorage(str(tmp_path))
    s.set("a","1")
    assert s.get("a") == "1"
    s.delete("a")
    assert s.get("a") is None

def test_recovery(tmp_path):
    s = LSMStorage(str(tmp_path))
    s.set("a","1")
    s2 = LSMStorage(str(tmp_path))
    assert s2.get("a") == "1"
