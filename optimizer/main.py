import optimizations
import cacheOptimization

application_code_path = "sample-code/pagerank_exp_0.scala"
spark_final_conf_path = "bin/spark-final.conf"
rdd_actions_path = "RDDActions.txt"
rdd_creations_path = "RDDCreations.txt"
rdd_creations_partitions_path = "RDDCreationsPartitions.txt"

#first find optimizations and insert rdd caching if needed
cache_optimized_code, optimization_report = cacheOptimization.cacheOptimization(application_code_path, rdd_actions_path, rdd_creations_path)
cache_optimized_code, optimization_report = optimizations.setParallelism(cache_optimized_code, rdd_creations_partitions_path, spark_final_conf_path, optimization_report)
spark_code_advise = optimizations.recommendReduceByKey(cache_optimized_code)
new_conf_file = optimizations.setMemoryFraction(cache_optimized_code, spark_final_conf_path)

#Generate optimization report
with open("output/optimization-report.txt", 'wr') as opt_report:
	opt_report.write(optimization_report)
#Generate the optimized code
with open("output/optimizedCode.scala", 'wr') as opt_code:
	opt_code.write(cache_optimized_code)
#Generate the new configuration settings file
with open("output/spark.final.conf.updated", 'wr') as spark_conf_update:
	spark_conf_update.write(new_conf_file)
#Generate the recommendations report
with open("output/spark.code.advise", 'wr') as advise:
	advise.write(spark_code_advise)
