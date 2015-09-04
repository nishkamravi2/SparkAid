import optimizations as op
import cacheOptimization
import sys
import os.path
import shutil

#remove intermediate tmp application code file path
script_dir = os.path.dirname(__file__)
application_code_path = open(os.path.join(script_dir, "../../../bin/tmp-code-file-path.txt")).read()
os.remove(os.path.join(script_dir, "../../../bin/tmp-code-file-path.txt"))

#load data from auto-config and data folder
spark_final_conf_path =  os.path.join(script_dir, "../../../bin/output/spark-final.conf")
rdd_actions_path = os.path.join(script_dir, "data/RDDActions.txt")
rdd_creations_path = os.path.join(script_dir, "data/RDDCreations.txt")
rdd_creations_partitions_path = os.path.join (script_dir, "data/RDDCreationsPartitions.txt")

spark_final_conf = open(spark_final_conf_path).read()
rdd_actions_file = open(rdd_actions_path).read()
rdd_creations_file = open(rdd_creations_path).read()
rdd_creations_partitions_file = open(rdd_creations_partitions_path).read()

application_code = open(application_code_path).read()

optimized_code, optimization_report = cacheOptimization.cacheOptimization(application_code, rdd_actions_file, rdd_creations_file)
optimized_code, optimization_report = op.setParallelism(optimized_code, rdd_creations_partitions_file, spark_final_conf, optimization_report)
spark_code_advice , optimization_report = op.recommendReduceByKey(optimized_code, optimization_report)
new_conf_file, optimization_report = op.setMemoryFraction(optimized_code, spark_final_conf, rdd_actions_file, rdd_creations_file, optimization_report)

#remove pre-existing output files
output_folder_path = os.path.join(script_dir, "../../../bin/output/*")
if os.path.exists(output_folder_path):
	shutil.rmtree(output_folder_path)

#Generate optimization report
with open("../bin/output/optimization-report.txt", 'wr') as opt_report:
	opt_report.write(optimization_report)

#Generate the optimized code only if there was optimizations done
if optimized_code != application_code:
	with open("../bin/output/optimizedCode.scala", 'wr') as opt_code:
		opt_code.write(optimized_code)

#Generate the new configuration settings file
with open("../bin/output/spark-final.conf", 'wr') as spark_conf_update:
	spark_conf_update.write(new_conf_file)

#Generate the recommendations report only if there exists recommendations
if len(spark_code_advice) > 1:
	with open("../bin/output/spark-code.advice", 'wr') as advice:
		advice.write(spark_code_advice)
