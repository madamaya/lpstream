import redis

redis = redis.Redis(host='localhost', port=6379, db=0)
keys = redis.keys("*")

lst = []
for key in keys:
  try:
    value = redis.get(key)
    lst.append([value.decode('utf-8'), key.decode('utf-8')])
  except Exception as e:
    print(e)

lst.sort()

with open("pairs.csv", "w") as w:
  for e in lst:
    w.write("{},{}\n".format(e[0], e[1]))


