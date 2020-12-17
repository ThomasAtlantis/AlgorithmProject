def extendLink(link, flag):
	for k in range(13):
		for i in range(13):
			for j in range(13):
				if link[i][j] > link[i][k] + link[k][j]:
					link[i][j] = link[i][k] + link[k][j]
					flag[i][j] = k
def buildPath(i, j, flag, path):
	if i == j:
		return	
	elif flag[i][j] == -1:
		path.append(j)
	else:
		buildPath(i, flag[i][j], flag, path)
		buildPath(flag[i][j], j, flag, path)
def testLink(link):
	for i in range(13):
		for j in range(13):
			print("%.2e " % link[i][j], end="")
		print()
	print()

if __name__ == '__main__':
	link = [[1e10 for j in range(13)] for i in range(13)]
	flag = [[-1   for j in range(13)] for i in range(13)]
	path = [[[]   for j in range(13)] for i in range(13)]
	with open("bandwidth.dat", "r") as reader:
		for line in reader.readlines():
			dc_1, dc_2, bandwidth = line.strip().split()
			dc_1, dc_2, bandwidth = int(dc_1.strip()[2:]), int(dc_2.strip()[2:]), int(bandwidth.strip())
			link[dc_1 - 1][dc_2 - 1] = 100 / bandwidth
	testLink(link)
	extendLink(link, flag)
	testLink(link)
	for i in range(13):
		for j in range(13):
			buildPath(i, j, flag, path[i][j])
			path[i][j] = path[i][j][:-1]
			print(f"{i}->{j}: {path[i][j]}")