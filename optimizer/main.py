import optimizations
import cacheOptimization
import sys
import os.path

script_dir = os.path.dirname(__file__)
application_code_path = open(os.path.join(script_dir, "../bin/code.file.path")).read()
spark_final_conf_path =  os.path.join(script_dir, "../bin/output/spark-final.conf")
rdd_actions_path = os.path.join(script_dir, "bin/RDDActions.txt")
rdd_creations_path = os.path.join(script_dir, "bin/RDDCreations.txt")
rdd_creations_partitions_path = os.path.join (script_dir, "bin/RDDCreationsPartitions.txt")

spark_final_conf = open(spark_final_conf_path).read()
rdd_actions_file = open(rdd_actions_path).read()
rdd_creations_file = open(rdd_creations_path).read()
rdd_creations_partitions_file = open(rdd_creations_partitions_path).read()
application_code = open(application_code_path).read()

cache_optimized_code, optimization_report = cacheOptimization.cacheOptimization(application_code, rdd_actions_file, rdd_creations_file)
cache_optimized_code, optimization_report = optimizations.setParallelism(cache_optimized_code, rdd_creations_partitions_file, spark_final_conf, optimization_report)
spark_code_advise = optimizations.recommendReduceByKey(cache_optimized_code)
# cache_optimized_code = cacheOptimization.commentRemover(open(application_code_path).read())
new_conf_file = optimizations.setMemoryFraction(cache_optimized_code, spark_final_conf, rdd_actions_file, rdd_creations_file)

#Generate optimization report
with open("../bin/output/optimization-report.txt", 'wr') as opt_report:
	opt_report.write(optimization_report)
#Generate the optimized code
with open("../bin/output/optimizedCode.scala", 'wr') as opt_code:
	opt_code.write(cache_optimized_code)
#Generate the new configuration settings file
with open("../bin/output/spark.final.conf", 'wr') as spark_conf_update:
	spark_conf_update.write(new_conf_file)
#Generate the recommendations report
with open("../bin/output/spark.code.advise", 'wr') as advise:
	advise.write(spark_code_advise)
