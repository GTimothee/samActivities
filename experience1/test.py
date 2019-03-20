import json
with open("runs.json") as jsonfile:
	args = json.load(jsonfile)
test=dict(args['samples_1']["config"])
print(test['strategies'])
