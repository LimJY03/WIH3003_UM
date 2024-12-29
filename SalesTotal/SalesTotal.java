import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.io.IOException;

public class SalesTotal {
	
	public static enum Counter {
	    RECORD_COUNT
	}
	
	public static class SalesMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text category = new Text();
        private DoubleWritable price = new DoubleWritable();
        
        public SalesMapper() {
            super();
        }
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] fields = value.toString().split(",");
                String productCategory = fields[18];  
                double productPrice = Double.parseDouble(fields[16]);  
    
                category.set(productCategory);
                price.set(productPrice);
                
                context.getCounter(Counter.RECORD_COUNT).increment(1);
    
                // Emit the category and price
                context.write(category, price);
            } catch (Exception e) {
                // Handle any errors with malformed records
            }
        }
        
	}

    public static class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable totalSales = new DoubleWritable();
        
        public SalesReducer() {
            super();
        }
       
    
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
    
            // Sum the prices for each category
            for (DoubleWritable value : values) {
                sum += value.get();
            }
    
            totalSales.set(sum);
    
            // Emit the total sales for the category
            context.write(key, totalSales);
        }
    }
	
    public static void main(String[] args) throws Exception {

        // Check if the correct number of arguments are passed
        if (args.length != 2) {
            System.err.println("Usage: SalesTotal <input path> <output path>");
            System.exit(-1);
        }

        // Start the timer for execution time
        long startTime = System.nanoTime();

        // Set up the configuration and job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sales Total per Category");

        job.setJarByClass(SalesTotal.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);

        // Set the output types (key: category, value: total sales)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and wait for completion
        boolean jobStatus = job.waitForCompletion(true);
        
        // Metrics Calculation
        if (jobStatus) {
            // End the timer for execution time
            long endTime = System.nanoTime();
            double duration = (endTime - startTime) / 1e9d;
            System.out.printf("Execution Time: %.3f seconds\n", duration);
            
            // Calculate memory usage
            Runtime runtime = Runtime.getRuntime();
            double usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0);
            System.out.printf("Memory Used: %.3f MB\n", usedMemory);
            
            // Get memory statistics from the JVM
            MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
            double usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0);
            double usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed() / (1024.0 * 1024.0);
            System.out.printf("Heap Memory Used: %.3f MB\n", usedHeapMemory);
            System.out.printf("Non-Heap Memory Used: %.3f MB\n", usedNonHeapMemory);
            
            // Get the record count from the job's counters
            long recordCount = job.getCounters().findCounter(Counter.RECORD_COUNT).getValue();
            double throughput = recordCount / duration;
            System.out.printf("Throughput: %.3f records per second\n", throughput);
        } else {
            System.exit(1);
        }
        
        System.exit(0);
	}

}