f = open("english.stop")

dict = {}
for line in f:
    # print(line.rstrip("\n"))
    temp = line.rstrip("\n")
    dict[temp] = 1
    # dict[temp.upper()] = 1
    # dict[temp.title()] = 1

f.close()

with open("user-ct-test-collection-01.txt") as f:
    for line in f:
        line = line.rstrip("\n")
        line = line.split("\t")
        keywords = line[1]
        keywords = keywords.split()
        keywords = [x for x in keywords if not(x.lower() in dict)]
        keywords = " ".join(keywords)
        if (keywords != ""):
            line[1] = keywords
        else:
            line[1] = ""
        line = "\t".join(line)
        if (line != ""):
            print(line)
