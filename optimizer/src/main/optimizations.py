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

def trimmedSpan(matched_obj):
	current_span = matched_obj.span()
	line = matched_obj.group()
	l_trimed_line = line.lstrip()
	num_white_spaces = len(line) - len(l_trimed_line) 

	return (current_span[0] + num_white_spaces, current_span[1])


def inComment(matched_obj, code_body, comments_span_list = None):
	if comments_span_list == None:
		comments_span_list = findCommentSpans(code_body)
	line_span = trimmedSpan(matched_obj)
	start_index = line_span[0]
	commentFlag = False

	for i in range(len(comments_span_list)):
		if start_index >= comments_span_list[i][0] and start_index <= comments_span_list[i][1]:
			commentFlag = True

	return commentFlag

def recommendReduceByKey(application_code, optimization_report):
	comments_span_list = findCommentSpans(application_code)

	f = application_code.split("\n")
	advice_file = "===================== GroupByKey() Recommendation ====================\n"
	optimization_report += "\n===================== GroupByKey() Recommendation ====================\n"
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
	#change to finditer instead
	matched_action_iter = re.finditer(r'^\s*(%s)\.(cache|persist)' %rdd, application_code, re.X|re.M)
	matched_assign_iter = re.finditer(r'(%s)\s*?=[^=]*?((\=\>)[^\n\n]*)*?\.(cache|persist)' %rdd, application_code, re.S|re.X|re.M)

	uncommented_cached_rdd_set = set()

	for matched_obj in matched_action_iter:
		if not inComment(matched_obj, application_code, comments_span_list):
			uncommented_cached_rdd_set.add(matched_obj.group(1))

	for matched_obj in matched_assign_iter:
		if not inComment(matched_obj, application_code, comments_span_list):
			uncommented_cached_rdd_set.add(matched_obj.group(1))

	return len(uncommented_cached_rdd_set) >= 1

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

def setMemoryFraction(application_code, spark_final_conf, rdd_actions, rdd_creations, optimization_report):
	comments_span_list = findCommentSpans(application_code)
	rdd_patterns = '|'.join(rdd_actions.split('\n') + rdd_creations.split('\n'))
	f = application_code.split("\n") #delete this line
	rdd_set = findAllRDDs(application_code, rdd_patterns)
	persistFlag = False
	for rdd in rdd_set:
		if isCached(rdd, comments_span_list, application_code):
			persistFlag = True
	if (not persistFlag):
		optimization_report += "\n===================== Storage Memory Fraction Optimization ===========\n"
		optimization_report += "\nspark.storage.memoryFraction set to 0.1 since there are no RDDs being persisted/cached.\n" #return this out later.
		return changeSettingValue("spark.storage.memoryFraction", "0.1", spark_final_conf), optimization_report

	return spark_final_conf, optimization_report

def setParallelism(application_code, rdd_creations_partitions, spark_final_conf, optimization_report):
	comments_span_list = findCommentSpans(application_code)
	default_parallelism = getSettingValue("spark.default.parallelism", spark_final_conf)
	pattern_list = '|'.join(rdd_creations_partitions.split("\n"))
	matched_iter = re.finditer(r'''	# captures textFile("anything")
		^.*?				
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

	parallelism_report = ""

	if matched_iter:
		for matched_obj in matched_iter:
			if inComment(matched_obj, application_code):
				continue
			line = matched_obj.group()
			recommended_line = line.rsplit(")",1)
			recommended_line = recommended_line[0] + ", " + str(default_parallelism) + ")" + recommended_line[1]
			parallelism_report += "Modified from: \n" + line + "\n"
			parallelism_report += "To:\n" + recommended_line + "\n\n"
			application_code = application_code.replace(line, recommended_line)

	if len(parallelism_report):
		optimization_report += "\n===================== Parallelism Optimization =======================\n"
		optimization_report += parallelism_report

	return application_code, optimization_report
