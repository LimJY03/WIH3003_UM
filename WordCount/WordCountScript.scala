import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._

val startTime = System.nanoTime()
val input = sc.textFile("file:////home/cloudera/Downloads/final_data.csv")
val rows = input.map(line => line.split(","))
val numberOfRecords = rows.count()
println(s"Number of records: $numberOfRecords")
val words = rows.flatMap(_.flatMap(_.split("\\s+")))
val wordPairs = words.map(word => (word, 1))
val wordCounts = wordPairs.reduceByKey(_ + _)
wordCounts.collect().foreach { case (word, count) => println(s"$word: $count") }
val endTime = System.nanoTime()

val executionTime = (endTime - startTime) / 1e9
val throughput = numberOfRecords / executionTime
val runtime = Runtime.getRuntime
val usedMemory = (runtime.totalMemory - runtime.freeMemory) / (1024.0 * 1024.0)
val memoryMXBean = ManagementFactory.getMemoryMXBean
val heapMemoryUsage = memoryMXBean.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0)
val nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage.getUsed / (1024.0 * 1024.0)

println(s"Spark Execution Time: $executionTime seconds")
println(s"Throughput: $throughput records per second")
println(s"Heap Memory Usage: $heapMemoryUsage MB")
println(s"Non Heap Memory Usage: $nonHeapMemoryUsage MB")