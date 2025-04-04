package gr.aueb.panagiotisl.mapreduce.wordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCount {

    // Create a logger instance
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    // Mapper class
    public static class CountMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            
            // Skip header or rows with insufficient fields
            if (fields.length < 16) {
                logger.info("Skipping line: insufficient fields (" + fields.length + "): " + line);  
                return;
            }
            if (fields[0].equalsIgnoreCase("year")) {
                logger.info("Skipping header row: " + line);  
            }

            try {
                
                String seller = fields[12].trim();
                String saleDate = fields[15].trim();

                double sellingPrice = Double.parseDouble(fields[14].trim());
                double mmr = Double.parseDouble(fields[13].trim());

                if (sellingPrice <= 0 || mmr <= 0) {
                    logger.info("Skipping line due to non-positive prices: " + line);  
                    return;
                }

                double difference = sellingPrice - mmr;

                String[] dateParts = saleDate.split(" ");
                if (dateParts.length < 4) {
                    logger.error("Invalid date format in line: " + line);  
                    return;
                }
                String yearMonth = dateParts[3] + "-" + getMonthNumber(dateParts[1]);
                String outputKey = seller + ":" + yearMonth;
            
                context.write(new Text(outputKey + ":" + fields[12].trim()), new DoubleWritable(difference));

            } catch (Exception e) {
                logger.error("Error processing line: " + line, e);  
            }
        }

        // Helper method to convert month names to two-digit numbers
        private String getMonthNumber(String month) {
            switch (month.toLowerCase()) {
                case "jan": return "01";
                case "feb": return "02";
                case "mar": return "03";
                case "apr": return "04";
                case "may": return "05";
                case "jun": return "06";
                case "jul": return "07";
                case "aug": return "08";
                case "sep": return "09";
                case "oct": return "10";
                case "nov": return "11";
                case "dec": return "12";
                default: return "00";
            }
        }
    }

        // Reducer class
        public static class CountReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
           
            double maxDifference = Double.MIN_VALUE;
            double totalDifference = 0.0;
            int count = 0;
            String carWithMaxDifference = "";

            // Sum up the differences and find the maximum
            for (DoubleWritable value : values) {
                double diff = value.get();
                if (diff > maxDifference) {
                    maxDifference = diff;
                    carWithMaxDifference = value.toString(); // Store the corresponding car name
                }
                totalDifference += diff;
                count++;
            }

            if (count == 0) {
                logger.info("No values received for key: " + key.toString()); 
                return;
            }

            double averageDifference = totalDifference / count;
            String result = carWithMaxDifference + ": " + String.format("%.1f", maxDifference) 
                            + ", avg: " + String.format("%.1f", averageDifference);

            // Emit the result for the key
            context.write(key, new Text(result));
        }
    }

}
