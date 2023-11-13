from sys import argv

if len(argv) < 2:
    print(f"usage {argv[1]} filename")
    exit(1)
filename=argv[1]
lines=[]
now=[]
filters = argv[2:]

def push(now):
    texts = "".join(now)
    for x in filters:
        if x[0]=="!":
            if x[1:] in texts:
                return
        else:
            if x not in texts:
                return
    try:
        t = int(now[-1])
        lines.append((t, now))
    except:
        pass

with open(filename) as f:
    for line in f:
        line=line.strip()
        if len(line) == 0:
            push(now)
            now=[]
        else:
            now.append(line)
push(now)

lines.sort(reverse=True)
for x in lines:
    print("\n".join(x[1]))
    print()
