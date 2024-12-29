import java.lang.management.ManagementFactory

val startTime = System.nanoTime()
val linesRDD = sc.textFile("hdfs:///input/final_data.csv")
val header = linesRDD.first()
val dataRDD = linesRDD.filter(line => line != header).map(line => line.split(","))
val cityOrderRDD = dataRDD.map(fields => (fields(8), 1))
val cityOrderCounts = cityOrderRDD.reduceByKey(_ + _)
cityOrderCounts.saveAsTextFile("hdfs:///sparkOut_5")
val endTime = System.nanoTime()
val duration = (endTime- startTime) / 1e9d
println(s"Execution Time: $duration seconds")
val recordCount = dataRDD.count()
val throughput = recordCount / duration
println(s"Throughput: $throughput records per second")
val memoryMXBean = ManagementFactory.getMemoryMXBean()
val usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed / (1024.0 * 1024.0)
val usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed / (1024.0 * 1024.0)
println(s"Used Heap Memory: $usedHeapMemory MB")
println(s"Used Non-Heap Memory: $usedNonHeapMemory MB")