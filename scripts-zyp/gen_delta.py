from sys import stdin
from re import findall

p1="before bootstrap ([^ ]*) ms"
p2="bootstrap success: ([^ ]*) ms"
p3="unit create done: .*, time ([^ ]*) ms"
p4="resource pool create done: .*, time ([^ ]*) ms"
p5="tenant create done: .*, time ([^ ]*) ms"

texts = ''.join(stdin.readlines())
times = []
for x in [p1,p2,p3,p4,p5]:
    res = findall(x, texts)
    if len(res) == 0:
        print(f"shit, '{x}' not found")
        exit(1)
    times.append(float(res[0]))

pre=0
for x in zip(["before bootstrap", "bootstrap", "test unit", "resource pool create", "tenant create"], times):
    print(x[0], x[1], x[1]-pre, sep=',')
    pre = x[1]
