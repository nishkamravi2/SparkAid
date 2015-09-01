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

def findCommentSpans(code_body):
    matched_comment_iter = re.finditer(
        r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"',
        code_body, re.DOTALL | re.MULTILINE | re.X
        )
    comments_span_list = []
    for matched_com_obj in matched_comment_iter:
    	if matched_com_obj.group().startswith('/'):
        	comments_span_list.append(matched_com_obj.span())

    comments_span_list = sorted(comments_span_list, key = lambda x: x[0])
    return comments_span_list

def inComment(matched_obj, code_body, comments_span_list = None):
	if comments_span_list == None:
		comments_span_list = findCommentSpans(code_body)

	line_span = matched_obj.span()
	start_index = line_span[0]
	commentFlag = False

	for i in range(len(comments_span_list)):
		if start_index > comments_span_list[i][0] and start_index <= comments_span_list[i][1]:
			commentFlag = True

	return commentFlag

def recommendReduceByKey(application_code, optimization_report):
	comments_span_list = findCommentSpans(application_code)

	f = application_code.split("\n")
	advice_file = "===================== GroupByKey() Recommendation ========================\n"
	optimization_report += "\n===================== GroupByKey() Recommendation ========================\n"
	recommendFlag = False

	matched_iter = re.finditer(r'''
		[^\s]*\.groupByKey.*$
		''', application_code, re.M|re.X)

	for matched_obj in matched_iter:
		if not inComment(matched_obj, application_code, comments_span_list):
			line = matched_obj.group()
			advice_file += "Consider using reduceByKey() instead of groupByKey() if possible at: \n" + line + "\n\n"
			recommendFlag = True

	if not recommendFlag:
		advice_file += "\nNo advice for this code\n" 
		optimization_report += "\n"
	else:
		optimization_report += "\nSee spark-code.advice file\n"

	return advice_file, optimization_report

def isCached(rdd, comments_span_list, application_code):
	#add find iter
	matched_action = re.search(r'(%s)\.(cache|persist)' %rdd, application_code, re.X|re.M)
	matched_assign = re.search(r'^[^/]*?(%s)\s*?=[^=]*?((\=\>)[^\n\n]*)*?\.(cache|persist)' %rdd, application_code, re.S|re.X|re.M)

	if matched_action:
		if inComment(matched_action, application_code, comments_span_list):
			matched_action = None

	if matched_assign:
		if inComment(matched_assign, application_code, comments_span_list):
			matched_assign = None

	return matched_action is not None or matched_assign is not None

def findAllRDDs(application_code, rdd_patterns):
	comments_span_list = findCommentSpans(application_code)
	rdd_set = set()
	matched_iter = re.finditer(r'(val|var)\s*([^=]*?)\s*?=[^=]*?\.(%s)'%rdd_patterns, application_code, re.S|re.X|re.M)
	if matched_iter:
		for matched_obj in matched_iter:
			line = matched_obj.group()
			if not inComment(matched_obj, application_code, comments_span_list):
				rddname = matched_obj.group(2)
				rdd_set.add(rddname) 
	return rdd_set

def setMemoryFraction(application_code, spark_final_conf, rdd_actions, rdd_creations):
	comments_span_list = findCommentSpans(application_code)
	optimization_report = "\n===================== Storage Memory Fraction Optimization ========================\n"
	rdd_patterns = '|'.join(rdd_actions.split('\n') + rdd_creations.split('\n'))
	f = application_code.split("\n") #delete this line
	rdd_set = findAllRDDs(application_code, rdd_patterns)
	persistFlag = False
	for rdd in rdd_set:
		if isCached(rdd, comments_span_list, application_code):
			persistFlag = True
	if (not persistFlag):
		optimization_report += "spark.storage.memoryFraction set to 0.1 since there are no RDDs being persisted/cached.\n" #return this out later.
		return changeSettingValue("spark.storage.memoryFraction", "0.1", spark_final_conf)
	return spark_final_conf

def setParallelism(application_code, rdd_creations_partitions, spark_final_conf, optimization_report):
	comments_span_list = findCommentSpans(application_code)
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
			if inComment(matched_obj, application_code):
				continue
			line = matched_obj.group()
			recommended_line = line.rsplit(")",1)
			recommended_line = recommended_line[0] + ", " + str(default_parallelism) + ")" + recommended_line[1]
			optimization_report += "Modified from: " + line + "\n"
			optimization_report += "To this: " + recommended_line + "\n\n"
			application_code = application_code.replace(line, recommended_line)

	return application_code, optimization_report
