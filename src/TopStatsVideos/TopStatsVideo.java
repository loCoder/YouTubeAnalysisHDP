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

public class TopStatsVideo extends Configured implements Tool {
    private static long start, end;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TopStatsVideo(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        start = new Date().getTime();
        Job job1 = Job.getInstance(getConf(), "removeDup");
        Job job2 = Job.getInstance(getConf(), "sortByViews");
        Job job3 = Job.getInstance(getConf(), "sortByLikes");
        Job job4 = Job.getInstance(getConf(), "sortByDislikes");
        Job job5 = Job.getInstance(getConf(), "sortByComments");

        job1.setJarByClass(TopStatsVideo.class);
        job1.setMapperClass(MapProc.class);
        job1.setReducerClass(ReduceProc.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "Inter_1"));
        job1.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 1 took " + (end - start) + "milliseconds\n");

        job2.setJarByClass(TopStatsVideo.class);
        job2.setMapperClass(MapViews.class);
        job2.setReducerClass(ReduceSort.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "Inter_1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "viewStats"));
        job2.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 2 took " + (end - start) + "milliseconds\n");

        job3.setJarByClass(TopStatsVideo.class);
        job3.setMapperClass(MapLikes.class);
        job3.setReducerClass(ReduceSort.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[1] + "Inter_1"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "likeStats"));
        job3.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 3 took " + (end - start) + "milliseconds\n");

        job4.setJarByClass(TopStatsVideo.class);
        job4.setMapperClass(MapDislikes.class);
        job4.setReducerClass(ReduceSort.class);
        job4.setMapOutputKeyClass(LongWritable.class);
        job4.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(args[1] + "Inter_1"));
        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "dislikesStats"));
        job4.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 3 took " + (end - start) + "milliseconds\n");

        job5.setJarByClass(TopStatsVideo.class);
        job5.setMapperClass(MapComments.class);
        job5.setReducerClass(ReduceSort.class);
        job5.setMapOutputKeyClass(LongWritable.class);
        job5.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job5, new Path(args[1] + "Inter_1"));
        FileOutputFormat.setOutputPath(job5, new Path(args[1] + "commentStats"));


        boolean status = job5.waitForCompletion(true);
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
                            long viewCount = Long.parseLong(nextLine[7].trim());
                            long likeCount = Long.parseLong(nextLine[8].trim());
                            long dislikeCount = Long.parseLong(nextLine[9].trim());
                            long commentCount = Long.parseLong(nextLine[10].trim());
                            if (vidURL.equals("#NAME?") || vidURL.equals("#VALUE!")) { //for URL N/A
                                currentKey = new Text(vidName);
                                currentVal = new Text(vidURL + "\t" + viewCount + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount);
                            } else { //for removing rudundant vids of changed titlenames
                                currentKey = new Text(vidURL);
                                currentVal = new Text(vidName + "\t" + viewCount + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount);

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
            String cVal, stat = "", token1 = "", token2;
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
                    token1 = nextLine[0];
                }

                reader.close();

            }
            token2 = word.toString().trim();
            if (token1.equals("#NAME?") || token1.equals("#VALUE!"))
                stat = "N/A" + "\t" + token2 + "\t" + stat;
            else
                stat = token2 + "\t" + token1 + "\t" + stat;
            newWord = new Text(stat);
            context.write(newWord, new LongWritable(max));

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
                        if (nextLine.length == 6 && !(nextLine[0].trim().equals("video_id"))) {
                            String vidURL = nextLine[0].trim();
                            String vidName = nextLine[1].trim();
                            String viewCount = nextLine[5].trim();
                            String likeCount = nextLine[2].trim();
                            String dislikeCount = nextLine[3].trim();
                            String commentCount = nextLine[4].trim();
                            currentKey = new LongWritable(Long.parseLong(viewCount) * -1);
                            //viewInteger=Long.parseLong(nextLine[7].trim());
                            //views = new LongWritable(viewInteger);
                            currentVal = new Text(vidURL + "\t" + vidName + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount);
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

    public static class MapLikes extends Mapper < LongWritable, Text, LongWritable, Text > {
        private LongWritable views = new LongWritable();
        private Text word = new Text();
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
                        if (nextLine.length == 6 && !(nextLine[0].trim().equals("video_id"))) {
                            String vidURL = nextLine[0].trim();
                            String vidName = nextLine[1].trim();
                            String viewCount = nextLine[5].trim();
                            String likeCount = nextLine[2].trim();
                            String dislikeCount = nextLine[3].trim();
                            String commentCount = nextLine[4].trim();
                            currentKey = new LongWritable(Long.parseLong(likeCount) * -1);
                            //viewInteger=Long.parseLong(nextLine[7].trim());
                            //views = new LongWritable(viewInteger);
                            currentVal = new Text(vidURL + "\t" + vidName + "\t" + viewCount + "\t" + dislikeCount + "\t" + commentCount);
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

    public static class MapDislikes extends Mapper < LongWritable, Text, LongWritable, Text > {
        private LongWritable views = new LongWritable();
        private Text word = new Text();
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
                        if (nextLine.length == 6 && !(nextLine[0].trim().equals("video_id"))) {
                            String vidURL = nextLine[0].trim();
                            String vidName = nextLine[1].trim();
                            String viewCount = nextLine[5].trim();
                            String likeCount = nextLine[2].trim();
                            String dislikeCount = nextLine[3].trim();
                            String commentCount = nextLine[4].trim();
                            currentKey = new LongWritable(Long.parseLong(dislikeCount) * -1);
                            currentVal = new Text(vidURL + "\t" + vidName + "\t" + viewCount + "\t" + likeCount + "\t" + commentCount);
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

    public static class MapComments extends Mapper < LongWritable, Text, LongWritable, Text > {
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
                        if (nextLine.length == 6 && !(nextLine[0].trim().equals("video_id"))) {
                            String vidURL = nextLine[0].trim();
                            String vidName = nextLine[1].trim();
                            String viewCount = nextLine[5].trim();
                            String likeCount = nextLine[2].trim();
                            String dislikeCount = nextLine[3].trim();
                            String commentCount = nextLine[4].trim();
                            currentKey = new LongWritable(Long.parseLong(commentCount) * -1);
                            //viewInteger=Long.parseLong(nextLine[7].trim());
                            //views = new LongWritable(viewInteger);
                            currentVal = new Text(vidURL + "\t" + vidName + "\t" + viewCount + "\t" + likeCount + "\t" + dislikeCount);
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