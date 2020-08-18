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

public class TopViewedChannels extends Configured implements Tool {
    private static long start, end;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TopViewedChannels(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        start = new Date().getTime();
        Job job1 = Job.getInstance(getConf(), "removeDup");
        Job jobMid = Job.getInstance(getConf(), "mapChan");
        Job job2 = Job.getInstance(getConf(), "sortByViews");

        job1.setJarByClass(TopViewedChannels.class);
        job1.setMapperClass(MapProc.class);
        job1.setReducerClass(ReduceProc.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "Inter_1"));
        job1.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 1 took " + (end - start) + "milliseconds\n");

        jobMid.setJarByClass(TopViewedChannels.class);
        jobMid.setMapperClass(MapChan.class);
        jobMid.setReducerClass(ReduceChan.class);
        jobMid.setMapOutputKeyClass(Text.class);
        jobMid.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobMid, new Path(args[1] + "Inter_1"));
        FileOutputFormat.setOutputPath(jobMid, new Path(args[1] + "Inter_2"));
        jobMid.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 1 took " + (end - start) + "milliseconds\n");

        job2.setJarByClass(TopViewedChannels.class);
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
                            long likeCount = Long.parseLong(nextLine[8].trim());
                            long dislikeCount = Long.parseLong(nextLine[9].trim());
                            long commentCount = Long.parseLong(nextLine[10].trim());
                            if (vidURL.equals("#NAME?") || vidURL.equals("#VALUE!")) { //for URL N/A
                                currentKey = new Text(vidName);
                                currentVal = new Text(vidURL + "\t" + viewCount + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount + "\t" + chName);
                            } else { //for removing rudundant vids of changed titlenames
                                currentKey = new Text(vidURL);
                                currentVal = new Text(vidName + "\t" + viewCount + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount + "\t" + chName);

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


    public static class ReduceProc extends Reducer < Text, Text, Text, LongWritable > {
        //@Override
        public void reduce(Text word, Iterable < Text > vals, Context context)
        throws IOException,
        InterruptedException {

            long ccount;
            long max = 0;
            String cVal, stat = "", chName = "", token2;
            Text newWord = new Text();
            //taking latest record and removing duplicates
            for (Text val: vals) {
                cVal = val.toString();
                CSVReader reader = new CSVReader(new StringReader(cVal), '\t', '"', 0);
                String[] nextLine = reader.readNext();
                ccount = Long.parseLong(nextLine[1].trim());
                if (ccount > max) {
                    max = ccount;
                    stat = nextLine[2] + "\t" + nextLine[3] + "\t" + nextLine[4];
                    chName = nextLine[5];
                }
                reader.close();
            }
            stat = chName + "\t" + stat;
            newWord = new Text(stat);
            context.write(newWord, new LongWritable(max));

        }
    }

    public static class MapChan extends Mapper < LongWritable, Text, Text, Text > {
        private LongWritable views = new LongWritable();
        private Text word = new Text();
        private long numRecords = 0;
        private long viewInteger;


        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            Text currentKey = new Text();
            Text currentVal;
            if (line.length() > 0) {
                CSVReader reader = new CSVReader(new StringReader(line), '\t', '"', 0);
                String[] nextLine;
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 5 && !(nextLine[0].trim().equals("video_id"))) {
                            String chName = nextLine[0].trim();
                            String viewCount = nextLine[4].trim();
                            String likeCount = nextLine[1].trim();
                            String dislikeCount = nextLine[2].trim();
                            String commentCount = nextLine[3].trim();
                            currentKey = new Text(chName);
                            //viewInteger=Long.parseLong(nextLine[7].trim());
                            //views = new LongWritable(viewInteger);
                            currentVal = new Text(likeCount + "\t" + dislikeCount + "\t" + commentCount + "\t" + viewCount);
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

    public static class ReduceChan extends Reducer < Text, Text, Text, Text > {
        @Override
        public void reduce(Text word, Iterable < Text > vals, Context context)
        throws IOException,
        InterruptedException {
            long comments = 0, likes = 0, dislikes = 0, views = 0;
            for (Text val: vals) {
                CSVReader reader = new CSVReader(new StringReader(val.toString().trim()), '\t', '"', 0);
                String[] nextLine = reader.readNext();
                likes += Long.parseLong(nextLine[0]);
                dislikes += Long.parseLong(nextLine[1]);
                comments += Long.parseLong(nextLine[2]);
                views += Long.parseLong(nextLine[3]);
                reader.close();

            }
            String sumStats = likes + "\t" + dislikes + "\t" + comments + "\t" + views;
            context.write(word, new Text(sumStats));
        }
    }

    public static class MapViews extends Mapper < LongWritable, Text, LongWritable, Text > {
        private LongWritable views = new LongWritable();
        private Text word = new Text();
        private long numRecords = 0;
        private long viewInteger;


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
                        if (nextLine.length == 5 && !(nextLine[0].trim().equals("video_id"))) {
                            String chName = nextLine[0].trim();
                            String viewCount = nextLine[4].trim();
                            String likeCount = nextLine[1].trim();
                            String dislikeCount = nextLine[2].trim();
                            String commentCount = nextLine[3].trim();
                            currentKey = new LongWritable(Long.parseLong(viewCount) * -1);
                            currentVal = new Text(chName + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount);
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
        public void reduce(LongWritable word, Iterable < Text > vals, Context context)
        throws IOException,
        InterruptedException {
            long max = -1 * word.get();
            word = new LongWritable(max);
            for (Text val: vals)
                context.write(word, val);

        }
    }


}