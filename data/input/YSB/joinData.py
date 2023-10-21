import json

# mapping作成
mp = {}
with open("pairs.csv") as f:
  while True:
    line = f.readline()
    if line == "":
      break

    elements = line.split(",")

    assert len(elements) == 2

    mp[elements[1].replace("\n", "")] = elements[0].replace("\n", "")

# Join
count = 0
with open("generated.json") as f:
  with open("ysb.json", "w") as w:
    while True:
      line = f.readline()
      if line == "":
        break
      count = count + 1

      j_data = json.loads(line)

      j_data["campaign_id"] = mp[j_data["ad_id"]]

      w.write("{}\n".format(json.dumps(j_data)))

      if count % 100000 == 0:
        print(count)