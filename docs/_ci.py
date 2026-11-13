
import json, urllib.request

d = json.load(open("/tmp/j.json", encoding="utf-8"))
win = [j for j in d["jobs"] if "windows" in j["name"]]
if not win:
    print("未找到 windows job")
else:
    jid = win[0]["id"]
    print("job id:", jid)
    # 从 steps 里找失败的
    for s in win[0]["steps"]:
        if s["conclusion"] != "success":
            print("失败步骤:", s["name"])
