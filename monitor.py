import time, Pyro5.api

NODE_IDS = [1, 2, 3, 4]

while True:
    rows = []
    for nid in NODE_IDS:
        try:
            with Pyro5.api.Proxy(f"PYRONAME:raft.node{nid}") as p:
                p._pyroTimeout = 0.3
                rows.append(p.get_status())
        except Exception as e:
            rows.append({"node_id": nid, "state": f"DOWN ({type(e).__name__})"})
    print("\033[2J\033[H", end="")  # clear screen
    for r in rows:
        print(f"  node{r['node_id']:>1}  {r.get('state','?'):<10}  "
            f"term={r.get('term','?')}  leader={r.get('leader_id','?')}")
    time.sleep(0.3)