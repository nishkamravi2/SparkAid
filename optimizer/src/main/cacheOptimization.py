import optimizations as opt
import re

def getLoopBodyIndex(indexes, application_code):
	"""
	Get loop body indexes from the application code 
	"""
	body_indexes = []
	last_closing_index = -1 #this is to take care of nested loops to prevent overlapping analysis
	for i in range(len(indexes)):
		start_index = indexes[i][0]
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

def getLoopPatternPosition(loop_patterns, application_code):
	"""
	Gets the position of loop regex (for/while/do) occurence in code
	"""
	loop_keyword_positions = []
	for keyword in loop_patterns:
		matched_iter = re.finditer(keyword, application_code, re.S)
		loop_keyword_positions += [matched_obj.span() for matched_obj in matched_iter]
	return loop_keyword_positions

def getLoopBodyAndSpan(loop_body_indexes, application_code):
	"""
	Gets the loop body and index from indexes
	"""
	return [(application_code[span[0]:span[1]],span) for span in loop_body_indexes]

def findReassignedRDD(body, pattern_list, comments_span_list):
	"""
	Finds reassigned RDDs in a body of code
	"""
	reassigned_candidates = set()
	matched_iter = re.finditer(r'.*(%s)\s+=\s+\w+' %pattern_list, body, re.S)
	if matched_iter:
		for matched_obj in matched_iter:
			if opt.inComment(matched_obj, body):
				continue
			reassigned_candidates.add(matched_obj.group(1))
	return reassigned_candidates

def generateSpaceBuffer(length):
	"""
	Generates space buffer for inserting rdd.cache() with indentation
	""" 
	space_buffer = ""
	for i in range(length):
		space_buffer += " "
	return space_buffer

def generateCachedCode(cache_candidates, prev_line):
	"""
	Generates rdd.cache() for each rdd in cache_candidates and outputs cache flag 
	"""
	leading_spaces = len(prev_line.expandtabs(4)) - len(prev_line.expandtabs(4).lstrip())
	generated_code = "" #this is rdd1.cache(), rdd2.cache() ...
	cache_opt_flag = 0

	for rdd in cache_candidates:
		cached_line = generateSpaceBuffer(leading_spaces) + rdd + ".cache()" + "\n"
		generated_code += cached_line
		cache_opt_flag = 1

	return generated_code, cache_opt_flag

def getPrevNonEmptyLine(loop_line_num, list_application_code):
	"""
	Obtains the line number of the first presceding non-empty line
	"""
	prev_line_num = loop_line_num - 2
	line = list_application_code[prev_line_num]
	while (line is not None and len(line) == 0):
		prev_line_num -= 1
		line = list_application_code[prev_line_num]
	return max(prev_line_num, 0)

def generateApplicationCode (application_code, loop_line_num, cache_candidates, optimization_report):
	"""
	Generates new application code with cache inserts, and an optimization report
	"""
	list_application_code = application_code.split("\n")
	if loop_line_num >= len(list_application_code):
		loop_line_num = 0
	prev_line_num = getPrevNonEmptyLine(loop_line_num, list_application_code)
	generated_code, cache_opt_flag = generateCachedCode(cache_candidates, list_application_code[prev_line_num])

	if cache_opt_flag == 1:
		optimization_report += "Inserted code block at Line: " + str(loop_line_num) + "\n" + generated_code + "\n"
		application_code = '\n'.join(list_application_code[:loop_line_num - 1]) + '\n' + generated_code + '\n'.join(list_application_code[loop_line_num - 1:])

	return application_code, optimization_report

def extractLoopBodyAndPositions(application_code, loop_patterns):
	"""
	Extracts loop body strings and their corresponding position indexes
	"""
	#sort positions by starting positions
	loop_keyword_positions = sorted(getLoopPatternPosition(loop_patterns, application_code), key=lambda x: x[0])
	#find all loop body indexes
	loop_body_positions = getLoopBodyIndex(loop_keyword_positions, application_code)
	#get all loop body code
	loop_tuple_list = getLoopBodyAndSpan(loop_body_positions, application_code)
	return loop_tuple_list


def getRDDsFromLoops(loop, rdd_actions):
	"""
	finds all RDD candidates from loop and returns it as a set
	"""
	comments_span_list = opt.findCommentSpans(loop)
	rdd_set = set()
	non_arg_matched_iter = re.finditer(r'(\w+?)\.(%s)'%rdd_actions, loop, re.S|re.X|re.M)
	for matched_obj in non_arg_matched_iter:
		if not opt.inComment(matched_obj, loop, comments_span_list):
			rddname = matched_obj.group(1)
			rdd_set.add(rddname) 

	arg_matched_iter = re.finditer(r'(%s)\(\s*(\w+?)\s*\)'%rdd_actions, loop, re.S|re.X|re.M)
	for matched_obj in arg_matched_iter:
		if not opt.inComment(matched_obj, loop, comments_span_list):
			rddname = matched_obj.group(2)
			rdd_set.add(rddname) 

	return rdd_set

def removeCachedRDDs(cache_candidate_set, application_code, end_limit):
	"""
	Removes cached rdds from set that occur before end_limit in application_code
	"""
	comments_span_list = opt.findCommentSpans(application_code)
	filtered_cache_candidates = set()
	for rdd in cache_candidate_set:
		if opt.isCached(rdd, comments_span_list, application_code, end_limit) == False:
			filtered_cache_candidates.add(rdd)
	return filtered_cache_candidates

def initBeforeLoop(application_code, rdd, end_limit):
	"""
	Finds all the rdd var names in the code
	"""
	search_region = application_code[:end_limit]
	comments_span_list = opt.findCommentSpans(search_region)
	rdd_set = set()
	matched_iter = re.finditer(r'(val|var)\s*(%s)\s*?='%rdd, search_region, re.S|re.X|re.M)
	for matched_obj in matched_iter:
		if not opt.inComment (matched_obj, search_region):
			rdd_set.add(matched_obj.group())
	return len(rdd_set) > 0

def removeRDDsInitBeforeLoop(application_code, cache_candidate_set, loop_start_index):
	"""
	Removes RDDs that were not initalized before the loop
	"""
	filtered_set = set()
	for candidate in cache_candidate_set:
		if initBeforeLoop(application_code, candidate, loop_start_index):
			filtered_set.add(candidate)
	return filtered_set

def removeReassignedRDDs(loop, cache_candidate_set):
	"""
	Removes reassigned RDDs from set within the loop
	"""
	rdd_candidate_regex_pattern = '|'.join(cache_candidate_set)
	comments_span_list = opt.findCommentSpans(loop)
	cache_candidate_set.difference_update(findReassignedRDD(loop, rdd_candidate_regex_pattern, comments_span_list))

	return cache_candidate_set

def cacheOptimization(application_code, rdd_actions, rdd_creations):
	"""
	Finds RDDs that should be cached and inserts the code to do so
	"""
	optimization_report = "===================== Cache Optimization =============================\n"
	updated_opt_report = optimization_report
	new_application_code = application_code
	rdd_actions = '|'.join(rdd_actions.split("\n")) # (a|b|c)
	loop_patterns = [r'for\s*\(.+?\)\s*\{', r'while\s*\(.+?\)\s*\{', r'do\s*\{.*\}']
	loop_tuple_list = extractLoopBodyAndPositions(application_code, loop_patterns) #change to positions
	loop_line_num_offset = 0 #this is to help to offset line numbers when additional code is added
	global_cache_set = set()

	for loop_tuple in loop_tuple_list:
		loop = loop_tuple[0]
		loop_start_index = loop_tuple[1][0]
		#get RDDs from loop
		cache_candidate_set = getRDDsFromLoops(loop, rdd_actions)
		#remove re-assigned RDDs
		cache_candidate_set = removeReassignedRDDs(loop, cache_candidate_set)
		#remove cached RDDs
		cache_candidate_set = removeCachedRDDs(cache_candidate_set, application_code, loop_start_index)
		#remove RDDs not initialized outside, and before the loop
		cache_candidate_set = removeRDDsInitBeforeLoop(application_code, cache_candidate_set, loop_start_index)
		#remove RDDs that have been already been cached through our optimizations in a prior loop
		cache_candidate_set.difference_update(global_cache_set)
		#update the set of RDDs cached through our optimizations
		global_cache_set.update(cache_candidate_set)
		#get loop line number
		loop_line_num = opt.getLineNumber(loop_start_index, application_code) 
		#generate application code
		new_application_code, updated_opt_report = generateApplicationCode(new_application_code, loop_line_num + loop_line_num_offset, cache_candidate_set, updated_opt_report)
		#update line insertion offset for optimized code
		loop_line_num_offset += len(cache_candidate_set)

	if updated_opt_report == optimization_report:
		updated_opt_report += "No cache optimizations done.\n"

	return new_application_code, updated_opt_report
