import java.lang.management.ManagementFactory

val startTime = System.nanoTime()
val linesRDD = sc.textFile("hdfs:///sparkdata1")
val header = linesRDD.first()
val dataRDD = linesRDD.zipWithIndex().filter { case (_, idx) => idx != 0 }.map(_._1)
val categoryPriceRDD = dataRDD.flatMap { line =>
   val fields = line.split(",")
   if (fields.length > 18 && fields(16).matches("\\d+(\\.\\d+)?")) {
       Some((fields(18), fields(16).toDouble))
   } else {
       None
   }
}
val totalSalesPerCategoryRDD = categoryPriceRDD.reduceByKey(_ + _)
val outputDir = "TotalSalesOut1"
val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
totalSalesPerCategoryRDD.saveAsTextFile(outputDir)
val endTime = System.nanoTime()
val duration = (endTime - startTime) / 1e9d
println(s"Execution Time: $duration seconds")
val recordCount = dataRDD.count()
val throughput = recordCount / duration
println(s"Throughput: $throughput records per second")
val memoryMXBean = ManagementFactory.getMemoryMXBean()
val usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed / (1024.0 * 1024.0)
val usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed / (1024.0 * 1024.0)
println(s"Used Heap Memory: $usedHeapMemory MB")
println(s"Used Non-Heap Memory: $usedNonHeapMemory MB")