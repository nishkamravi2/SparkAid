import re
import cacheOptimization

def getSettingValue(key, conf):
	f = conf.split('\n')
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
		if "groupByKey()" in f[i]:
			advise_file += "Consider using reduceByKey() instead of groupByKey() if possible in " + "Line " + str(i+1) + ": " + f[i] + "\n"
	if len(advise_file) == 0:
		advise_file += "No advise for this code"
	return advise_file

def isCached(rdd, application_code):
	matched_action = re.search(r'(%s)\.(cache|persist)' %rdd, application_code, re.X)
	matched_assign = re.search(r'(%s)\s*?=[^=]*?((\=\>)[^\n\n]*)*?\.(cache|persist)' %rdd, application_code, re.S|re.X)
	return matched_action is not None or matched_assign is not None

def findAllRDDs(application_code, rdd_patterns):
	rdd_set = set()
	matched_iter = re.finditer(r'(val|var)\s*([^=]*?)\s*?=[^=]*?\.(%s)'%rdd_patterns, application_code, re.S|re.X)
	if matched_iter:
		for matched_obj in matched_iter:
			rddname = matched_obj.group(2)
			rdd_set.add(rddname) 
	return rdd_set

def setMemoryFraction(application_code, spark_final_conf, rdd_actions, rdd_creations):
	rdd_patterns = '|'.join(rdd_actions.split('\n') + rdd_creations.split('\n'))
	f = application_code.split("\n")
	rdd_set = findAllRDDs(application_code, rdd_patterns)
	persistFlag = False
	for rdd in rdd_set:
		if isCached(rdd, application_code):
			persistFlag = True
	if (not persistFlag):
		print "Setting spark.storage.memoryFraction to 0.1 since there are no RDDs being persisted/cached."
		return changeSettingValue("spark.storage.memoryFraction", "0.1", spark_final_conf)
	return spark_final_conf

def changeSettingValue(key, new_value, settings_file):
	f = settings_file.split("\n")
	for i in range(0, len(f)):
		if key in f[i]:
			old_value = f[i].split()[1]
			f[i] = f[i].replace(old_value, new_value)
			break
	return "\n".join(f)

def setParallelism(application_code, rdd_creations_partitions, spark_final_conf, optimization_report):
	optimization_report += "\n=====================Parallelism Optimizations========================\n"
	default_parallelism = getSettingValue("spark.default.parallelism", spark_final_conf)
	pattern_list = '|'.join(rdd_creations_partitions.split("\n"))
	matched_iter = re.finditer(r'''	# captures textFile("anything")
		.*				# any char starting
		\s*=\s*\w+
		\.(%s)			# .parallelize|objectFile|textFile|...
		\(			
		(
			\w*
			|\s*\".*\"\s* # explicit file path only  -  "/path/to/file"
			|\s*\w+\s* # file path as a var name only - path-var-name
			|\s*\".*\"\s*\,\s*[^\)\(\,]*\s*\,\s*[^\)\(\,]*\s*\,\s*[^\)\(\,]*\s* # 4 arguments explicit file path - "/path/to/file" , 3 args
			|\s*\w*\s*\,\s*[^\)\(\,]*\s*\,\s*[^\)\(\,]*\s*\,\s*[^\)\(\,]*\s* # 4 arguments path-var-name - path-var-name, 3 args
		)
		\)
		''' %pattern_list, application_code, re.X)

	if matched_iter:
		for matched_obj in matched_iter:
			line = matched_obj.group()
			recommended_line = line.rsplit(")",1)
			recommended_line = recommended_line[0] + ", " + str(default_parallelism) + ")" + recommended_line[1]
			optimization_report += "Modified from: " + line + "\n"
			optimization_report += "To this: " + recommended_line + "\n\n"
			application_code = application_code.replace(line, recommended_line)

	return application_code, optimization_report
