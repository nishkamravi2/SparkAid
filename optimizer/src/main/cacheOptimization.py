import optimizations as opt
import re

def getLoopBodySpans(keyword_positions, application_code, func_spans):
	"""
	Get loop body spans from the application code and keyword_positions
	"""
	body_spans = []
	for i in range(len(keyword_positions)):
		start_pos = keyword_positions[i][0]
		end_pos = -1
		stack = []
		for j in range(start_pos,len(application_code)):
			curr_char = application_code[j]
			if curr_char == "{":
				stack.append(curr_char)
				if start_pos == keyword_positions[i][0]: 
					start_pos = j
			elif curr_char == "}":
				stack.pop()	 
				if (len(stack)==0):
					end_pos = j+1
					break;
		span = (start_pos, end_pos)
		if (opt.inFunctionDeclHelper(span, application_code, func_spans)):
			body_spans.append((start_pos,end_pos))
	return body_spans

def getLoopPatternPosition(loop_patterns, application_code, func_spans):
	"""
	Gets the position of loop regex (for/while/do) occurence in code
	"""
	loop_keyword_positions = []
	for keyword in loop_patterns:
		matched_iter = re.finditer(keyword, application_code, re.S)
		for matched_obj in matched_iter:
			if not opt.inComment(matched_obj, application_code) and opt.inFunctionDecl(matched_obj, application_code, func_spans):
				loop_keyword_positions += [matched_obj.span()]
	return loop_keyword_positions

def getLoopBodyAndSpan(loop_body_spans, application_code):
	"""
	Gets the loop body and spans from keyword_positions
	"""
	return [(application_code[span[0]:span[1]],span) for span in loop_body_spans]

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
	cache_candidates = [cand for cand in cache_candidates]
	for i in range(len(cache_candidates)):
		rdd = cache_candidates[i]
		cached_line = generateSpaceBuffer(leading_spaces) + rdd + ".cache()"
		if i != len(cache_candidates) - 1:
			cached_line += "\n"
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

def generateLineInsertList (application_code, loop_line_num, cache_candidates, line_insert_list):
	"""
	Generates new application code with cache inserts, and an optimization report
	"""
	list_application_code = application_code.split("\n")
	if loop_line_num >= len(list_application_code):
		loop_line_num = 0
	prev_line_num = getPrevNonEmptyLine(loop_line_num, list_application_code)
	generated_code, cache_opt_flag = generateCachedCode(cache_candidates, list_application_code[prev_line_num])

	if cache_opt_flag == 1:
		# optimization_report += "Inserted code block at Line: " + str(loop_line_num) + "\n" + generated_code + "\n"
		# application_code = '\n'.join(list_application_code[:loop_line_num - 1]) + '\n' + generated_code + '\n'.join(list_application_code[loop_line_num - 1:])
		line_insert_list.append((loop_line_num, generated_code, len(cache_candidates)))

	return line_insert_list

def extractLoopBodyAndPositions(application_code, loop_patterns, func_spans):
	"""
	Extracts loop body strings and their corresponding position indexes
	"""
	#sort positions by starting positions
	loop_keyword_positions = sorted(getLoopPatternPosition(loop_patterns, application_code, func_spans), key=lambda x: x[0])
	#find all loop body spans
	loop_body_spans = getLoopBodySpans(loop_keyword_positions, application_code, func_spans)
	#get all loop body code
	loop_tuple_list = getLoopBodyAndSpan(loop_body_spans, application_code)
	return loop_tuple_list


def getRDDsFromLoops(loop, rdd_actions, rdd_functions):
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

	#this is to capture functions defined that are not default RDD functions
	for rdd_func in rdd_functions:
		func_name = rdd_func[0]
		num_args = rdd_func[4]
		arg_pos_array = rdd_func[5]

		num_periods = num_args - 1
		arg_regex_pattern = ""
		for i in range(num_args):
			#adds accordingly number of arg patterns to capture
			arg_regex_pattern += "\s*(\w+?)\s*"
			if i < num_args -1 :
				arg_regex_pattern += ","

		func_arg_matched_iter = re.finditer(r"""
			{0}\s*\({1}\)
			""".format(func_name,arg_regex_pattern) , loop, re.S|re.X|re.M)

		for matched_obj in func_arg_matched_iter:
			if not opt.inComment(matched_obj, loop, comments_span_list):
				#add in the corresponding arguments at their positions
				for arg_pos in arg_pos_array:
					rddname = matched_obj.group(arg_pos+1)
					rdd_set.add(rddname) 

	return rdd_set

def removeCachedRDDs(cache_candidate_set, application_code, end_limit, func_spans):
	"""
	Removes cached rdds from set that occur before end_limit in application_code
	"""
	comments_span_list = opt.findCommentSpans(application_code)
	filtered_cache_candidates = set()
	for rdd in cache_candidate_set:
		if opt.isCached(rdd, comments_span_list, application_code, end_limit, func_spans) == False:
			filtered_cache_candidates.add(rdd)
	return filtered_cache_candidates

def initBeforeLoop(application_code, rdd, end_limit, func_spans, func_rdd_args):
	"""
	Finds all the rdd var names in the code
	"""
	#Check if the args of the function was one of the candidate
	for rdd_arg in func_rdd_args:
		if rdd_arg == rdd:
			return True

	span_with_limit = opt.spansWithEndLimit(func_spans, end_limit)
	search_region = opt.extractSearchRegion(span_with_limit, application_code)
	comments_span_list = opt.findCommentSpans(search_region)
	rdd_set = set()
	matched_iter = re.finditer(r'(val|var)\s*(%s)\s*?='%rdd, search_region, re.S|re.X|re.M)
	for matched_obj in matched_iter:
		if not opt.inComment (matched_obj, search_region):
			rdd_set.add(matched_obj.group())
	return len(rdd_set) > 0

def removeRDDsInitBeforeLoop(application_code, cache_candidate_set, loop_start_position, func_spans, func_rdd_args):
	"""
	Removes RDDs that were not initialized before the loop
	"""
	filtered_set = set()
	for candidate in cache_candidate_set:
		if initBeforeLoop(application_code, candidate, loop_start_position, func_spans, func_rdd_args):
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

def generateNewApplicationCodeAndOptReport(application_code, line_insert_list):
	"""
	Generates optimized code and opt report
	"""
	optimization_report = "===================== Cache Optimization =============================\n"
	updated_opt_report = optimization_report
	line_insert_list = sorted(line_insert_list, key = lambda x: x[0])
	list_application_code = application_code.split("\n")
	line_num_offset = 0
	for i in range(len(line_insert_list)):
		loop_num = line_insert_list[i][0]
		generated_code = line_insert_list[i][1]
		offset = line_insert_list[i][2]
		list_application_code = list_application_code[:loop_num + line_num_offset - 1] + [generated_code] + list_application_code[loop_num + line_num_offset - 1:]
		updated_opt_report += "Inserted code block at Line: " + str(loop_num + line_num_offset) + "\n" + generated_code + "\n"
		line_num_offset += offset

	if updated_opt_report == optimization_report:
		updated_opt_report += "No cache optimizations done.\n"

	new_application_code = "\n".join(list_application_code)
	return new_application_code, updated_opt_report

def functionCacheOpt(application_code, rdd_actions, rdd_functions, func_spans, func_rdd_args):
	"""
	Finds RDDs that should be cached and inserts the code to do so
	"""
	rdd_actions = '|'.join(rdd_actions.split("\n")) # (a|b|c)
	loop_patterns = [r'for\s*\(.+?\)\s*\{', r'while\s*\(.+?\)\s*\{', r'do\s*\{.*\}']
	loop_tuple_list = extractLoopBodyAndPositions(application_code, loop_patterns, func_spans) 
	
	global_cache_set = set()

	line_insert_list = []

	for loop_tuple in loop_tuple_list:
		loop = loop_tuple[0]
		loop_start_position = loop_tuple[1][0]
		#get RDDs from loop
		cache_candidate_set = getRDDsFromLoops(loop, rdd_actions, rdd_functions)
		#remove re-assigned RDDs
		cache_candidate_set = removeReassignedRDDs(loop, cache_candidate_set)
		#remove cached RDDs
		cache_candidate_set = removeCachedRDDs(cache_candidate_set, application_code, loop_start_position, func_spans) 
		#remove RDDs not initialized outside, and before the loop
		cache_candidate_set = removeRDDsInitBeforeLoop(application_code, cache_candidate_set, loop_start_position, func_spans, func_rdd_args) 
		#remove RDDs that have been already been cached through our optimizations in a prior loop
		cache_candidate_set.difference_update(global_cache_set)
		#update the set of RDDs cached through our optimizations
		global_cache_set.update(cache_candidate_set)
		#get loop line number
		loop_line_num = opt.getLineNumber(loop_start_position, application_code) 
		#generate line insertion list
		line_insert_list = generateLineInsertList(application_code, loop_line_num , cache_candidate_set, line_insert_list)
		
	return line_insert_list

###############################################################################


def getSpanFromStartPosition(start_pos, application_code):
	"""
	This gets the span of {} from a start position
	"""
	end_pos = -1
	stack = []
	for j in range(start_pos,len(application_code)):
	    curr_char = application_code[j]
	    if curr_char == "{":
	        stack.append(curr_char)
	    elif curr_char == "}":
	        stack.pop()  
	        if (len(stack)==0):
	            end_pos = j+1
	            break;
	return (start_pos, end_pos)

def hasRDDType(return_type_regex):
	"""
	Checks if return type is RDD type
	"""
	if "RDD" in return_type_regex or "rdd" in return_type_regex or "Rdd" in return_type_regex: 
	    return True
	return False

def getEffectiveSpan(spans_list):
	"""
	Given the list of function spans, this returns a list of function effective spans
	"""
	effective_span_list = []
	for i in range(len(spans_list)):
	    curr_range_list = []
	    curr_start_pos, curr_end_pos = spans_list[i] #current span we are looking at
	    #takes care of last span() case
	    if i == len(spans_list) - 1:
	        # if curr_start_pos > prev_end_pos:
	        curr_range_list.append( (curr_start_pos,curr_end_pos))

	    prev_start_pos, prev_end_pos = curr_start_pos, curr_end_pos
	    scanned_so_far_ptr = curr_start_pos

	    for j in range(i+1, len(spans_list)):
	        next_start_pos, next_end_pos = spans_list[j]
	        #if we have reached the next adjacent span 
	        if next_start_pos > curr_end_pos :
	            #wrap it up and break
	            curr_range_list.append( (scanned_so_far_ptr,curr_end_pos))
	            break
	        #if the next_end_pos is less than what we have already scanned, we ignore it
	        if next_end_pos < scanned_so_far_ptr:
	            if j == len(spans_list) -1:
	                curr_range_list.append( (scanned_so_far_ptr,curr_end_pos))
	            continue
	        #if the next_span is within curr_span:
	        if next_start_pos > scanned_so_far_ptr and next_end_pos < curr_end_pos:
	            #we want to add an interval from the curr_start_pos to next_start_pos
	            curr_range_list.append( (scanned_so_far_ptr,next_start_pos))
	            #keep track of previous excluded span to prevent counting spans already counted.
	            prev_start_pos, prev_end_pos = next_start_pos, next_end_pos
	            scanned_so_far_ptr = next_end_pos
	            #if reached end of the next list
	            if j == len(spans_list) -1:
	                curr_range_list.append( (scanned_so_far_ptr,curr_end_pos))
	    effective_span_list.append(curr_range_list)

	return effective_span_list


def extractFunctionData(application_code):
    """
    Extract functions and their meta data
    """
    closure_matched_iter = re.finditer(r"""
        def\s+
        ([\w_]+) # function name
        \s*
        \(
        (.*) # arguments of function
        \)
        (.*?)
        {    # start position of the function span
        """, application_code, re.X)

    no_closure_matched_iter = re.finditer(r"""
        def\s+
        ([\w_]+) # function name
        \s*
        \(
        (.*) # arguments of function
        \)
        (.*?)
        =    # take into account functions that do not have closures
        """, application_code, re.X)

    closure_function_list = []

    for matched_obj in closure_matched_iter:
        if not opt.inComment(matched_obj, application_code):
            function_name = matched_obj.group(1)
            arg_string = matched_obj.group(2)
            arg_array = map(str.strip, arg_string.split(","))
            arg_with_type_array = []
            rdd_type_arg_index = []
            for i in range(len(arg_array)):
            	arg = arg_array[i]
                arg_name, arg_type = map(str.strip, arg.split(":"))
                rdd_flag = hasRDDType(arg_type)
                #append arg name with its properties
                arg_with_type_array.append((arg_name,arg_type,rdd_flag))
                if hasRDDType(arg_type):
                	#appends arg positions of RDD type
                	rdd_type_arg_index.append(i)

            return_type_regex = matched_obj.group(3).strip()
            returnRDDFlag = hasRDDType(return_type_regex)
            regex_span = matched_obj.span()
            function_span = getSpanFromStartPosition(regex_span[1] - 1,application_code)
            num_args = len(arg_array)
            closure_function_list.append([function_name, arg_with_type_array, function_span, returnRDDFlag, num_args, rdd_type_arg_index])

    function_span_list = [func[2] for func in closure_function_list]
    effective_span_list = getEffectiveSpan(function_span_list)

    #reassign function_span to effective_span
    for i in range(len(effective_span_list)):
        closure_function_list[i][2] = effective_span_list[i]

    return closure_function_list

# def extractFunctionBody(func, application_code):
# 	"""
# 	Extracts function body from application code
# 	"""
# 	effective_span_list = func[2]
# 	function_body = ""
# 	last_span_added_line_num = 0
# 	for i in range(len(effective_span_list)):
# 		span = effective_span_list[i]
# 		function_body += application_code[span[0]:span[1]] 
# 	return function_body

def cacheOptimization(application_code, rdd_actions, rdd_creations):
	"""
	Main cache optimization function
	"""
	closure_function_list = extractFunctionData(application_code)
	master_line_insert_list = []

	for func in closure_function_list:
	    # function_body = extractFunctionBody(func, application_code)
	    func_spans = func[2]
	    func_rdd_args = func[1]
	    for args in func_rdd_args:
	    	arg_name = args[0]
	    	arg_type = args[1]
	    	if hasRDDType(arg_type):
	    		func_rdd_args.append(arg_name)
	    master_line_insert_list += functionCacheOpt(application_code, rdd_actions, closure_function_list, func_spans, func_rdd_args)

	optimized_code, optimization_report = generateNewApplicationCodeAndOptReport(application_code, master_line_insert_list)
	return optimized_code, optimization_report
