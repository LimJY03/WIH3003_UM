package bdaa;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

public class OrderByCity {

    // Mapper Class
    public static class CityOrderMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text city = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (!fields[0].equals("order_id")) { // Skip header
                if (fields.length > 8) {
                    city.set(fields[8]); // `customer_city` column
                    context.write(city, one);
                }
            }
        }
    }

    // Reducer Class
    public static class CityOrderReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: OrderByCity <input path> <output path>");
            System.exit(-1);
        }

        // Start time
        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Orders by Customer City");

        job.setJarByClass(OrderByCity.class);
        job.setMapperClass(CityOrderMapper.class);
        job.setReducerClass(CityOrderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for job completion
        boolean jobStatus = job.waitForCompletion(true);

        // End time
        long endTime = System.nanoTime();

        // Metrics calculation
        double duration = (endTime - startTime) / 1e9d; // in seconds
        System.out.println("Execution Time: " + duration + " seconds");

        // Throughput (records per second)
        long recordCount = job.getCounters()
                .findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
        double throughput = recordCount / duration;
        System.out.println("Throughput: " + throughput + " records per second");

        // Memory Usage
        Runtime runtime = Runtime.getRuntime();
        double usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0); // in MB
        System.out.println("Used Memory: " + usedMemory + " MB");

        // Heap and Non-Heap Memory
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        double usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0); // in MB
        double usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed() / (1024.0 * 1024.0); // in MB
        System.out.println("Used Heap Memory: " + usedHeapMemory + " MB");
        System.out.println("Used Non-Heap Memory: " + usedNonHeapMemory + " MB");

        // Exit
        System.exit(jobStatus ? 0 : 1);
    }
}
