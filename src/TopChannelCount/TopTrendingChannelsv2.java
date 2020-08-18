package org.myorg;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
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
    private static long start, end;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TopTrendingChannels(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        start = new Date().getTime();
        Job job1 = Job.getInstance(getConf(), "removeDup");
        Job jobMid = Job.getInstance(getConf(), "mapChan");
        Job job2 = Job.getInstance(getConf(), "sortByViews");

        job1.setJarByClass(TopTrendingChannels.class);
        job1.setMapperClass(MapProc.class);
        job1.setReducerClass(ReduceProc.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "Inter_1"));
        job1.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 1 took " + (end - start) + "milliseconds\n");

        jobMid.setJarByClass(TopTrendingChannels.class);
        jobMid.setMapperClass(MapChan.class);
        jobMid.setReducerClass(ReduceChan.class);
        jobMid.setMapOutputKeyClass(Text.class);
        jobMid.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(jobMid, new Path(args[1] + "Inter_1"));
        FileOutputFormat.setOutputPath(jobMid, new Path(args[1] + "Inter_2"));
        jobMid.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 1 took " + (end - start) + "milliseconds\n");

        job2.setJarByClass(TopTrendingChannels.class);
        job2.setMapperClass(MapViews.class);
        job2.setReducerClass(ReduceSort.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "Inter_2"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "viewStats"));

        boolean status = job2.waitForCompletion(true);
        if (status == true) {
            end = new Date().getTime();
            System.out.println("\nJob took " + (end - start) + "milliseconds\n");
            return 0;
        } else
            return 1;
    }

    public static class MapProc extends Mapper < LongWritable, Text, Text, Text > {
        private LongWritable views = new LongWritable();
        private Text word = new Text();
        private long numRecords = 0;
        private long viewInteger;
        //,likeInteger,dislikeInteger,comment_count;    

        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            Text currentKey = new Text(), currentVal;
            if (line.length() > 0) {
                CSVReader reader = new CSVReader(new StringReader(line));
                String[] nextLine;
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 16 && !(nextLine[0].trim().equals("video_id"))) {
                            String vidURL = nextLine[0].trim();
                            String vidName = nextLine[2].trim();
                            String chName = nextLine[3].trim();
                            long viewCount = Long.parseLong(nextLine[7].trim());
                            if (vidURL.equals("#NAME?") || vidURL.equals("#VALUE!")) { //for URL N/A
                                currentKey = new Text(vidName);
                                currentVal = new Text(vidURL + "\t" + viewCount + "\t" + chName);
                            } else { //for removing rudundant vids of changed titlenames
                                currentKey = new Text(vidURL);
                                currentVal = new Text(vidName + "\t" + viewCount + "\t" + chName);

                            }
                            context.write(currentKey, currentVal);
                        } else {
                            throw new Exception();
                        }

                    }
                } catch (NumberFormatException e) {} catch (NoClassDefFoundError e) {} catch (Exception e) {} finally {
                    reader.close();
                } //for single line
            }
        }
    }


    public static class ReduceProc extends Reducer < Text, Text, Text, IntWritable > {
        //@Override
        public void reduce(Text word, Iterable < Text > vals, Context context)
        throws IOException,
        InterruptedException {

            long ccount;
            int one = 1;
            long max = 0;
            String cVal, chName = "", token2;
            Text newWord = new Text();
            //taking latest record and removing duplicates
            for (Text val: vals) {
                cVal = val.toString();
                CSVReader reader = new CSVReader(new StringReader(cVal), '\t', '"', 0);
                String[] nextLine = reader.readNext();
                ccount = Long.parseLong(nextLine[1].trim());
                if (ccount > max) {
                    max = ccount;
                    chName = nextLine[2].trim();
                }
                reader.close();
            }
            newWord = new Text(chName);
            context.write(newWord, new IntWritable(one));

        }
    }

    public static class MapChan extends Mapper < LongWritable, Text, Text, IntWritable > {


        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            Text currentKey = new Text();
            IntWritable currentVal;
            if (line.length() > 0) {
                CSVReader reader = new CSVReader(new StringReader(line), '\t', '"', 0);
                String[] nextLine;
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 2 && !(nextLine[0].trim().equals("video_id"))) {
                            String chName = nextLine[0].trim();
                            int count = Integer.parseInt(nextLine[1].trim());
                            currentKey = new Text(chName);
                            currentVal = new IntWritable(count);
                            context.write(currentKey, currentVal);
                        } else {
                            throw new Exception();
                        }

                    }
                } catch (NumberFormatException e) {} catch (NoClassDefFoundError e) {} catch (Exception e) {} finally {
                    reader.close();
                } //for single line
            }
        }
    }

    public static class ReduceChan extends Reducer < Text, IntWritable, Text, IntWritable > {
        @Override
        public void reduce(Text word, Iterable < IntWritable > counts, Context context)
        throws IOException,
        InterruptedException {
            int ccount = 0;
            for (IntWritable count: counts)
                ccount += count.get();
            context.write(word, new IntWritable(ccount));
        }
    }

    public static class MapViews extends Mapper < LongWritable, Text, LongWritable, Text > {


        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            LongWritable currentKey = new LongWritable();
            Text currentVal;
            if (line.length() > 0) {
                CSVReader reader = new CSVReader(new StringReader(line), '\t', '"', 0);
                String[] nextLine;
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 2) {
                            String chName = nextLine[0].trim();
                            String count = nextLine[1].trim();
                            currentKey = new LongWritable(Long.parseLong(count) * -1);
                            currentVal = new Text(chName);
                            context.write(currentKey, currentVal);
                        } else {
                            throw new Exception();
                        }

                    }
                } catch (NumberFormatException e) {} catch (NoClassDefFoundError e) {} catch (Exception e) {} finally {
                    reader.close();
                } //for single line
            }
        }
    }

    public static class ReduceSort extends Reducer < LongWritable, Text, LongWritable, Text > {
	public static long counter = 0;
	public static long headerFlag=0;

        public void reduce(LongWritable word, Iterable < Text > vals, Context context)
        throws IOException,
        InterruptedException {
	    if(!headerFlag)
		//to include header(column description in file) - remove header[0] while processing
	    {
		headerFlag++;
		context.write(new LongWritable(),new Text("Count\tChannel_name"));
	    }
            long max = -1 * word.get();
            word = new LongWritable(max);
	    if(counter<5)// to print only top 5 in output 
	    {
      		for (Text val: vals)
		{
                    context.write(word, val);
		    counter++;
		}
	    }
        }
    }
}