import re
import os

def getSettingValue(key, conf):
	"""
	Reads setting value from conf file given a key
	"""
	f = conf.split('\n')
	value = 0
	for i in range (0, len(f)):
		if key in f[i]:
			line = f[i].split()
			value = int(line[1])
			break

	return value

def changeSettingValue(key, new_value, settings_file):
	"""
	Returns a new settings file with new value
	"""
	f = settings_file.split("\n")
	for i in range(0, len(f)):
		if key in f[i]:
			old_value = f[i].split()[1]
			f[i] = f[i].replace(old_value, new_value)
			break

	return "\n".join(f)

def findCommentSpans(code_body):
	"""
	Finds comment spans given a code body
	"""
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
	"""
	Helper function to remove leading white spaces in a span
	"""
	current_span = matched_obj.span()
	line = matched_obj.group()
	l_trimed_line = line.lstrip()
	num_white_spaces = len(line) - len(l_trimed_line) 

	return (current_span[0] + num_white_spaces, current_span[1])


def inComment(matched_obj, code_body, comments_span_list = None):
	"""
	Checks if a matched object is part of a comment
	"""
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
	"""
	Recommend reduceByKey instead of groupByKey if found in code 
	"""
	comments_span_list = findCommentSpans(application_code)

	f = application_code.split("\n")
	advice_file = "===================== GroupByKey() Recommendation ====================\n"
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
		advice_file = ""
	else:
		optimization_report += "\n===================== GroupByKey() Recommendation ====================\n" + \
							   "\nSee spark-code.advice file\n"

	return advice_file, optimization_report

def isCached(rdd, comments_span_list, application_code, end_limit = -1):
	"""
	Checks if an rdd is cached within code from 0 to end_limit
	"""
	if end_limit == -1:
		end_limit = len(application_code) - 1

	search_region = application_code[:end_limit]

	matched_action_iter = re.finditer(r'^\s*(%s)\.(cache|persist)' %rdd, search_region, re.X|re.M)
	matched_assign_iter = re.finditer(r'(%s)\s*?=[^=/]*?((\=\>)[^\n\n]*)*?\.(cache|persist)' %rdd, search_region, re.S|re.X|re.M)

	uncommented_cached_rdd_set = set()

	for matched_obj in matched_action_iter:
		if not inComment(matched_obj, search_region, comments_span_list):
			uncommented_cached_rdd_set.add(matched_obj.group(1))

	for matched_obj in matched_assign_iter:
		if not inComment(matched_obj, search_region, comments_span_list):
			uncommented_cached_rdd_set.add(matched_obj.group(1))

	return len(uncommented_cached_rdd_set) >= 1

def findAllRDDs(application_code, rdd_patterns):
	"""
	Finds all the rdd var names in the code
	"""
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
	"""
	Sets spark.storage.memoryFraction if no rdd cache instance exists
	"""
	comments_span_list = findCommentSpans(application_code)
	rdd_patterns = '|'.join(rdd_actions.split('\n') + rdd_creations.split('\n'))
	rdd_set = findAllRDDs(application_code, rdd_patterns)
	persistFlag = False
	for rdd in rdd_set:
		if isCached(rdd, comments_span_list, application_code):
			persistFlag = True
	if (not persistFlag):
		optimization_report += "\n===================== Storage Memory Fraction Optimization ===========\n" + \
							   "\nspark.storage.memoryFraction set to 0.1 since there are no RDDs being persisted/cached.\n"
		return changeSettingValue("spark.storage.memoryFraction", "0.1", spark_final_conf), optimization_report

	return spark_final_conf, optimization_report

def processRddCreationPartitionsPatternMethodNames(rdd_creations_partitions):
	"""
	Generates regex pattern for RDD creation names
	"""
	rdd_creations_partitions_list = rdd_creations_partitions.split("\n")
	pattern_list = '|'.join([x.split(",")[0] for x in rdd_creations_partitions_list])
	return pattern_list

def processRddCreationPartitionsPatternNumArgs(rdd_creations_partitions):
	"""
	Generates regex pattern for X number of args
	"""
	rdd_creations_partitions_list = rdd_creations_partitions.split("\n")
	num_args_set = set()

	for rdd_creation_tuple in rdd_creations_partitions_list:
		num_args = int(rdd_creation_tuple.split(',')[1])
		num_args_set.add(num_args - 2) #get the number of times we should append the pattern = maximum number of args the function can take minus 2

	final_arg_pattern = ""

	base_arg_pattern = '''
			|\s*\".*\"\s*
			|\s*\w+\s* 
	'''

	base_file_path_explicit_pattern = "|\s*\".*\"\s*"
	base_file_path_var_pattern = "|\s*\w+\s* "

	append_pattern = "\,\s*[^\)\(\,]*\s*"

	for num in num_args_set:
		new_explicit_pattern = base_file_path_explicit_pattern
		new_var_pattern = base_file_path_var_pattern
		for i in range (num):
			new_explicit_pattern +=  append_pattern 
			new_var_pattern += append_pattern
		final_arg_pattern += new_explicit_pattern  
		final_arg_pattern += new_var_pattern 

	return final_arg_pattern

def getLineNumber(start, application_code):
	"""
	Gets the line number of a given char index in the application code
	"""
	line_num = application_code.count(os.linesep, 0, start+1) + 1
	return line_num

def setParallelism(application_code, rdd_creations_partitions, spark_final_conf, optimization_report):
	"""
	Ensures RDD instantiation partitions is consistent with spark.default.parallelism
	"""
	comments_span_list = findCommentSpans(application_code)
	default_parallelism = getSettingValue("spark.default.parallelism", spark_final_conf)
	pattern_method_names_list = processRddCreationPartitionsPatternMethodNames(rdd_creations_partitions)
	pattern_num_args_list = processRddCreationPartitionsPatternNumArgs(rdd_creations_partitions)
	matched_iter = re.finditer(r'''	# captures textFile("anything")
		^.*?				
		\s*=\s*\w+
		\.({0})			# .parallelize|objectFile|textFile|...
		\(			
		(\w*{1})
		\)
		'''.format(pattern_method_names_list, pattern_num_args_list), application_code, re.X|re.M)

	parallelism_report = ""

	#a copy is made for determining comments as application_code is getting replaced every iteration of matching
	application_code_original = application_code
	for matched_obj in matched_iter:
		if inComment(matched_obj, application_code_original):
			continue
		line = matched_obj.group()
		recommended_line = line.rsplit(")",1)
		recommended_line = recommended_line[0] + ", " + str(default_parallelism) + ")" + recommended_line[1]

		line_num = getLineNumber(matched_obj.start(), application_code_original)

		parallelism_report += "At line " + str(line_num) + ":\n"+ \
							  "Modified from: \n" + line + "\n" + \
							  "To:\n" + recommended_line + "\n\n"
		application_code = application_code.replace(line, recommended_line)

	if len(parallelism_report):
		optimization_report += "\n===================== Parallelism Optimization =======================\n" + \
							   parallelism_report

	return application_code, optimization_report
