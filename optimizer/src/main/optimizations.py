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

def changeSettingValue(key, new_value, settings_file):
	f = settings_file.split("\n")
	for i in range(0, len(f)):
		if key in f[i]:
			old_value = f[i].split()[1]
			f[i] = f[i].replace(old_value, new_value)
			break
	return "\n".join(f)

#further implementation
def isComment(line, application_code):
	return False
	# line = re.escape(line)
	# print "line: ", line
	# matched = re.search(r'''//[^/]*?{0}[^/]*?'''.format(line), application_code, re.DOTALL| re.MULTILINE| re.X)
	# if matched:
	# 	print "matched result: ", matched.group()

	# print "=================================================================="
	# return matched is not None

#http://stackoverflow.com/questions/241327/python-snippet-to-remove-c-and-c-comments , not used for now
def removeComments(text):
    def replacer(match):
        s = match.group(0)
        if s.startswith('/'):
            return " " # note: a space and not an empty string
        else:
            return s
    pattern = re.compile(
        r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"',
        re.DOTALL | re.MULTILINE | re.X
    )
    return re.sub(pattern, replacer, text)

def inComment(matched_obj, application_code):
	line = matched_obj.group()
	return False

def recommendReduceByKey(application_code, optimization_report):
	f = application_code.split("\n")
	advise_file = "\n===================== GroupByKey() Recommendation ========================\n"
	optimization_report += "\n===================== GroupByKey() Optimization ========================\n"
	recommendFlag = False

	matched_iter = re.finditer(r'''
		^.*\.groupByKey.*$
		''', application_code, re.M|re.X)

	for matched_obj in matched_iter:
		if not inComment(matched_obj, application_code):
			line = matched_obj.group()
			advise_file += "Consider using reduceByKey() instead of groupByKey() if possible at: \n" + line
			recommendFlag = True

	if not recommendFlag:
		advise_file += "No advice for this code" 
		optimization_report += "\n"
	else:
		optimization_report += "See spark-code.advice file"

	return advise_file, optimization_report

def isCached(rdd, application_code):

	print "rdd", rdd, "|||||||||||||||||||before"
	matched_action = re.search(r'(%s)\.(cache|persist)' %rdd, application_code, re.X|re.M)
	matched_assign = re.search(r'^[^/]*?(%s)\s*?=[^=]*?((\=\>)[^\n\n]*)*?\.(cache|persist)' %rdd, application_code, re.S|re.X|re.M)

	if matched_action:
		line = matched_action.group()
		print "line: ", line
		if isComment(line, application_code):
			matched_action = None

	if matched_assign:
		line = matched_assign.group()
		print "line2: ",  line
		print "span: ", matched_assign.span()
		if isComment(line, application_code):
			matched_assign = None

	return matched_action is not None or matched_assign is not None

def findAllRDDs(application_code, rdd_patterns):
	rdd_set = set()
	matched_iter = re.finditer(r'(val|var)\s*([^=]*?)\s*?=[^=]*?\.(%s)'%rdd_patterns, application_code, re.S|re.X|re.M)
	if matched_iter:
		for matched_obj in matched_iter:
			line = matched_obj.group()
			if (not isComment(line, application_code)):
				# line = matched_obj.group()
				print "FINDING RDDS **%$%$%", line
				rddname = matched_obj.group(2)
				print matched_obj.group(2)
				rdd_set.add(rddname) 
	print rdd_set
	return rdd_set

def setMemoryFraction(application_code, spark_final_conf, rdd_actions, rdd_creations):
	optimization_report = "\n===================== Storage Memory Fraction Optimization ========================\n"
	rdd_patterns = '|'.join(rdd_actions.split('\n') + rdd_creations.split('\n'))
	f = application_code.split("\n") #delete this line
	rdd_set = findAllRDDs(application_code, rdd_patterns)
	persistFlag = False
	for rdd in rdd_set:
		if isCached(rdd, application_code):
			persistFlag = True
	if (not persistFlag):
		optimization_report += "spark.storage.memoryFraction set to 0.1 since there are no RDDs being persisted/cached.\n" #return this out later.
		return changeSettingValue("spark.storage.memoryFraction", "0.1", spark_final_conf)
	return spark_final_conf

def setParallelism(application_code, rdd_creations_partitions, spark_final_conf, optimization_report):
	optimization_report += "\n===================== Parallelism Optimization ========================\n"
	default_parallelism = getSettingValue("spark.default.parallelism", spark_final_conf)
	pattern_list = '|'.join(rdd_creations_partitions.split("\n"))
	matched_iter = re.finditer(r'''	# captures textFile("anything")
		^[^/]*				# removing comments
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
		''' %pattern_list, application_code, re.X|re.M)

	if matched_iter:
		for matched_obj in matched_iter:
			line = matched_obj.group()
			if isComment(line, application_code):
				continue
			recommended_line = line.rsplit(")",1)
			recommended_line = recommended_line[0] + ", " + str(default_parallelism) + ")" + recommended_line[1]
			optimization_report += "Modified from: " + line + "\n"
			optimization_report += "To this: " + recommended_line + "\n\n"
			application_code = application_code.replace(line, recommended_line)

	return application_code, optimization_report
