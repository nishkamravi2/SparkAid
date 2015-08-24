import re
import optimizations

#http://stackoverflow.com/questions/241327/python-snippet-to-remove-c-and-c-comments
def commentRemover(text):
    def replacer(match):
        s = match.group(0)
        if s.startswith('/'):
            #find the character it is in
            return " " # note: a space and not an empty string
        else:
            return s
    pattern = re.compile(
        r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"',
        re.DOTALL | re.MULTILINE
    )
    return re.sub(pattern, replacer, text)
    
def getBodyIndex(indexes, application_code):
	body_indexes = []
	last_closing_index = -1 #this is to take care of nested loops to prevent overlapping analysis
	for i in range(len(indexes)):
		start_index = indexes[i][0]
		if last_closing_index > start_index: #this is to take care of nested loops to prevent overlapping analysis
			continue
		end_index = -1
		stack = []
		for j in range(start_index,len(application_code)):
			curr_char = application_code[j]
			if curr_char == "{":
				stack.append(curr_char)
				if start_index == indexes[i][0]: #re-initialize start index
					start_index = j
			elif curr_char == "}":
				stack.pop()	 #error check for incorrect open close brackets
				if (len(stack)==0):
					end_index = j+1
					last_closing_index = end_index #this is to take care of nested loops to prevent overlapping analysis
					break;
		#error check for invalid open close (with end index)
		body_indexes.append((start_index,end_index))
	return body_indexes

def getKeywordIndex(loop_keywords, application_code):
	loop_keyword_indexes = []
	for keyword in loop_keywords:
		matched = re.finditer(keyword, application_code, re.S)
		loop_keyword_indexes += [m.span() for m in matched]
	return loop_keyword_indexes

def getBodyCodeList(loop_body_indexes, application_code):
	return [application_code[index[0]:index[1]] for index in loop_body_indexes]

def printBodyCode(loop_body_indexes, application_code):
	for index in loop_body_indexes:
		print application_code[index[0]:index[1]]
		print "\n\n================================================================================================================\n\n"

def isCached(rdd,f):
	#checks if the rdd calls cache()
	matched_action = re.search(r'\n\s*[^/]\s*%s\.(cache|persist)' %rdd, f, re.S)
	return matched_action is not None
	#check if cached at the instantiation
	# matched_creation = re.search(r'\n\s*[^/]\s*(val|var)\s+(%s)\s*=\s*.*\.(cache|persist)' %rdd, f, re.S)
	# return (matched_action is not None) or (matched_creation is not None)

#non-capturing group
def findRDD(application_code, rdd_actions_list, rdd_creations_list):
	f = application_code.split("\n")
	rdd_set = set()
	#finds RDDs by RDD actions
	for i in range(0,len(f)):
		#((space*)(val|var)(space*)(var-name)(space*)=(space*)(.*).(map|groupByKey|reduceByKey).*)
		matched = re.match(r'\s*(val|var)\s+(.+?)\s*=\s*.*\.(%s).*' %rdd_actions_list, f[i])
		if matched and not optimizations.isComment(f[i]):
			rdd = matched.group(2)
			if not isCached(rdd,application_code):
				rdd_set.add(rdd)

	#find RDDs by RDD creation
	for i in range(0,len(f)):
		# (var|val) (name) = (sc|somethingelse). (parallelize|objectFile|hadoopFile)
		matched = re.match(r'\s*(val|var)\s+(.+?)\s*=\s*(sc|.*).*\.(%s).*' %rdd_creations_list, f[i])
		# if matched:
		if matched and not optimizations.isComment(f[i]):
			rdd = matched.group(2)
			if not isCached(rdd,application_code):
				rdd_set.add(rdd)
	return rdd_set

def findRDDInBody(body, pattern_list): #implement commenting functionality, consider removing all comments right from the start
	cache_candidates = set()
	matched = re.finditer(r'(%s)[.\)]' %pattern_list, body, re.MULTILINE) #only find RDDs that will have actions
	if matched:
		for m in matched:
			cache_candidates.add(m.group(1))
	return cache_candidates

def getRDDOutsideLoops(rdd_set, body_code_list, rdd_actions_list, rdd_creations_list):
	for body in body_code_list:
		body_rdd_set = findRDD(body, rdd_actions_list, rdd_creations_list)
		rdd_set = rdd_set - body_rdd_set
	return rdd_set

def findReassignedRDD(body, pattern_list):
	reassigned_candidates = set()
	matched = re.finditer(r'.*(%s)\s+=\s+\w+' %pattern_list, body, re.S)
	if matched:
		for m in matched:
			reassigned_candidates.add(m.group(1))
	return reassigned_candidates

def findFirstLoopIndex(loop_keywords, application_code):
	loop_keyword_indexes = []
	f = application_code.split("\n")
	first_loop_index = len(f) + 1
	for keyword in loop_keywords:
		for i in range(0,len(f)):
			matched = re.search(keyword, f[i], re.S)
			if matched:
				if i < first_loop_index:
					first_loop_index = i 
	return first_loop_index + 1

def generateCachedCode(cache_candidates):
	cache_inserted_code = "//inserted new cache code below \n"
	for rdd in cache_candidates:
		cached_line = rdd + ".cache()" + "\n"
		cache_inserted_code += cached_line
	return cache_inserted_code

def generateApplicationCode (application_code, first_loop_index, cache_candidates, optimization_report):
	f = application_code.split("\n")
	generatedCode = generateCachedCode(cache_candidates)
	line_inserted = first_loop_index-1
	optimization_report += "Inserted code block at Line: " + str(line_inserted) + "\n" + generatedCode + "\n"
	f = '\n'.join(f[:first_loop_index-1]) + '\n' + generatedCode + '\n'.join(f[first_loop_index-1:])
	return f, optimization_report

def cacheOptimization(application_code_path, rdd_actions, rdd_creations):
	optimization_report = "=====================Cache Optimizations========================\n"
	application_code = commentRemover(open(application_code_path).read())
	rdd_actions_list = '|'.join(open(rdd_actions).read().split("\n")) 
	rdd_creations_list = '|'.join(open(rdd_creations).read().split("\n"))

	#find all RDDs in the code
	rdd_set = findRDD(application_code, rdd_actions_list, rdd_creations_list)
	#regex to capture for/while/do loops
	loop_keywords = [r'for\s*\(.+?\)\s*\{', r'while\s*\(.+?\)\s*\{', r'do\s*\{.*\}']
	#sort indexes by starting indexes to prevent overlap
	loop_keyword_indexes = sorted(getKeywordIndex(loop_keywords, application_code), key=lambda x: x[0])  #change this to linenumbers
	#find all loop body indexes
	loop_body_indexes = getBodyIndex(loop_keyword_indexes,application_code)
	#get all loop body code
	body_code_list = getBodyCodeList(loop_body_indexes, application_code) #collapse into one with loopbodyindexes.
	#
	rdd_body_set = getRDDOutsideLoops(rdd_set, body_code_list, rdd_actions_list, rdd_creations_list)
	regex_pattern = "|".join(rdd_body_set)

	cache_candidates = set()

	#for each body
	for body in body_code_list:
		cache_candidates.update(findRDDInBody(body, regex_pattern))
	for body in body_code_list:
		cache_candidates.difference_update(findReassignedRDD(body, regex_pattern))

	first_loop_index = findFirstLoopIndex(loop_keywords, application_code)
	new_application_code, optimization_report = generateApplicationCode(application_code, first_loop_index, cache_candidates, optimization_report)

	return new_application_code, optimization_report

#dependency for many different loops

