package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.StringReader;
import com.opencsv.CSVReader;

public class TopTrendingChannels extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TopTrendingChannels(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "trendchan");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper < LongWritable, Text, Text, IntWritable > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private long numRecords = 0;
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            Text currentWord = new Text();
            //v2ed!String [] keyvalue = line.split(",");
            //v2ed!String chName= keyvalue[3].trim();
            //v2ed!currentWord = new Text(chName);
            //v2ed!context.write(currentWord,one);
            //v2 start
            /*v3ed!for (String word : WORD_BOUNDARY.split(line)) {
        	if (word.isEmpty()) {
            		continue;
        	}
		String [] keyvalue = word.split(",");
		String chName= keyvalue[3].trim();
            currentWord = new Text(chName);
            context.write(currentWord,one);
        }!v3ed*/
            //v2 end
            //ed4
            if (line.length() > 0) {
                CSVReader reader = new CSVReader(new StringReader(line));
                String[] nextLine;
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 16) {
                            String chName = nextLine[3].trim();
                            currentWord = new Text(chName);
                            context.write(currentWord, one);
                        } else {
                            throw new Exception();
                        }

                    }
                } catch (NoClassDefFoundError e) {} catch (Exception e) {} finally {
                    reader.close();
                } //for single line
            }
        }
    }


    public static class Reduce extends Reducer < Text, IntWritable, Text, IntWritable > {
        @Override
        public void reduce(Text word, Iterable < IntWritable > counts, Context context)
        throws IOException,
        InterruptedException {
            int sum = 0;
            for (IntWritable count: counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));
        }
    }
}