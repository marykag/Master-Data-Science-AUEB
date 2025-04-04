package gr.aueb.panagiotisl.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        System.out.println("Driver started.");
        System.out.println("Number of arguments: " + args.length);

        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg[" + i + "]: " + args[i]);
        }

        // Determine if there is an extra first argument (main class name)
        int offset = (args.length == 3) ? 1 : 0;  

        // Ensure correct number of arguments (should be 2 after adjusting offset)
        if (args.length - offset != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }

        String inputPath = args[offset]; 
        String outputPath = args[offset + 1];

        System.out.println("Corrected Input Path: " + inputPath);
        System.out.println("Corrected Output Path: " + outputPath);

        // Instantiate a configuration
        Configuration configuration = new Configuration();

        // Instantiate a job
        Job job = Job.getInstance(configuration, "Word Count");

        // Set job parameters
        job.setJarByClass(Driver.class);
        job.setMapperClass(WordCount.CountMapper.class);
        job.setReducerClass(WordCount.CountReducer.class);

        // Specify output types for the Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class); 

        // Specify output types for the Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 

        // Set input format to TextInputFormat
        job.setInputFormatClass(TextInputFormat.class);

        // Set input and output paths dynamically
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Exit with job status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
