import re
import cacheOptimization

def getSettingValue(key, conf_path):
	f = open(conf_path).read().split('\n')
	parallelism = 0
	for i in range (0, len(f)):
		if key in f[i]:
			line = f[i].split()
			parallelism = int(line[1])
			break
	return parallelism

def isComment(line):
	matched = re.match(r'\s*//\s*.*',line)
	return matched is not None

def recommendReduceByKey(application_code):
	f = application_code.split("\n")
	advise_file = ""
	for i in range(0,len(f)):
		if "groupByKey()" in f[i] and not isComment(f[i]):
			advise_file += "Consider using reduceByKey() instead of groupByKey() if possible in " + "Line " + str(i+1) + ": " + f[i] + "\n"
	return advise_file

def setMemoryFraction(application_code, spark_final_conf_path):
	f = application_code.split("\n")
	settings_file = open(spark_final_conf_path).read()
	persistFlag = False
	for i in range(0,len(f)):
		if (("persist" in f[i] or "cache" in f[i])) and not isComment(f[i]):
			persistFlag = True
	if (not persistFlag):
		print "Setting spark.storage.memoryFraction to 0.1 since there are no RDDs being persisted/cached."
		return changeSettingValue("spark.storage.memoryFraction", "0.1", settings_file)
	return settings_file

def changeSettingValue(key, new_value, settings_file):
	f = settings_file.split("\n")
	for i in range(0, len(f)):
		if key in f[i]:
			old_value = f[i].split()[1]
			f[i] = f[i].replace(old_value, new_value)
			break
	return "\n".join(f)

def setParallelism(application_code, rdd_creations_partitions_path, spark_final_conf_path, optimization_report):
	optimization_report += "=====================Parallelism Optimizations========================\n"
	default_parallelism = getSettingValue("spark.default.parallelism", spark_final_conf_path)
	pattern_list = '|'.join(open(rdd_creations_partitions_path).read().split("\n"))
	matched_iter = re.finditer(r'''	# captures textFile("anything")
		.*				# start of string with any char starting
		\s*=\s*\w+
		\.(%s)			# .parallelize|objectFile|textFile|...
		\(			
		(
			\w*
			|\s*\".*\"\s*.* # explicit file path only  -  "/path/to/file"
			|\s*\w+\s* # file path as a var name only - path-var-name
			|\s*\".*\"\s*\,\s*.*\s*\,\s*.*\s*\,\s*.*\s* # 4 arguments explicit file path - "/path/to/file" , 3 args
			|\s*\w*\s*\,\s*.*\s*\,\s*.*\s*\,\s*.*\s* # 4 arguments path-var-name
		)
		\)			# (any char)
		.*?				# (anything after) ending at newline
		''' %pattern_list, application_code, re.X)
	if matched_iter:
		for m in matched_iter:
			line = m.group()
			recommended_line = line.rsplit(")",1)
			recommended_line = recommended_line[0] + ", " + str(default_parallelism) + ")" + recommended_line[1]
			optimization_report += "Modified from: " + line + "\n"
			optimization_report += "To this: " + recommended_line + "\n\n"
			application_code = application_code.replace(line, recommended_line)

	return application_code, optimization_report
