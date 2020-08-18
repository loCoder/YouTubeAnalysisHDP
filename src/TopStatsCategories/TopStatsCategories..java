package org.myorg;

import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.HashMap;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class TopStatsCategories extends Configured implements Tool {
    private static long start, end, end1;
    private static String region;
    public static HashMap < Integer, String > hmap;

    public static void main(String[] args) throws Exception {
        start = new Date().getTime();
        //an extra argument arg[2] to identify which dataset is being provided for individial DS analysis
        if (args[2].trim().length() != 2) {
            System.out.println("Please enter region name as 3rd arg correctly!\n Incorrect arg[2], exiting status:2");
            System.exit(2);
        }
        region = args[2].trim();
        hmap = new HashMap < Integer, String > ();
        FileReader fr = new FileReader("/home/woir/YTA/JAVA/json/" + region + "_category_id.json");
        Object obj = new JSONParser().parse(fr);
        JSONObject jo = (JSONObject) obj;
        JSONArray items = (JSONArray) jo.get("items");
        Iterator i = items.iterator();
        while (i.hasNext()) {
            JSONObject innerObj = (JSONObject) i.next();
            JSONObject structure = (JSONObject) innerObj.get("snippet");
            hmap.put(Integer.parseInt((String) innerObj.get("id")), (String) structure.get("title"));
        }
        fr.close();
        end1 = new Date().getTime();

        int res = ToolRunner.run(new TopStatsCategories(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Job job1 = Job.getInstance(getConf(), "removeDup");
        Job jobMid = Job.getInstance(getConf(), "mapCat");
        Job job2 = Job.getInstance(getConf(), "sortByViews");
        Job job3 = Job.getInstance(getConf(), "sortByLikes");
        Job job4 = Job.getInstance(getConf(), "sortByDislikes");
        Job job5 = Job.getInstance(getConf(), "sortByComments");

        job1.setJarByClass(TopStatsCategories.class);
        job1.setMapperClass(MapProc.class);
        job1.setReducerClass(ReduceProc.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "Inter_1"));
        job1.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 1 took " + (end - start) + "milliseconds\n");

        jobMid.setJarByClass(TopStatsCategories.class);
        jobMid.setMapperClass(MapCat.class);
        jobMid.setReducerClass(ReduceCat.class);
        jobMid.setMapOutputKeyClass(Text.class);
        jobMid.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobMid, new Path(args[1] + "Inter_1"));
        FileOutputFormat.setOutputPath(jobMid, new Path(args[1] + "Inter_2"));
        jobMid.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 1 took " + (end - start) + "milliseconds\n");

        job2.setJarByClass(TopStatsCategories.class);
        job2.setMapperClass(MapViews.class);
        job2.setReducerClass(ReduceSort.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "Inter_2"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "viewStats"));
        job2.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 2 took " + (end - start) + "milliseconds\n");

        job3.setJarByClass(TopStatsCategories.class);
        job3.setMapperClass(MapLikes.class);
        job3.setReducerClass(ReduceSort.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[1] + "Inter_2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "likeStats"));
        job3.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 3 took " + (end - start) + "milliseconds\n");

        job4.setJarByClass(TopStatsCategories.class);
        job4.setMapperClass(MapDislikes.class);
        job4.setReducerClass(ReduceSort.class);
        job4.setMapOutputKeyClass(LongWritable.class);
        job4.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(args[1] + "Inter_2"));
        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "dislikesStats"));
        job4.waitForCompletion(true);

        end = new Date().getTime();
        System.out.println("\nJob 3 took " + (end - start) + "milliseconds\n");

        job5.setJarByClass(TopStatsCategories.class);
        job5.setMapperClass(MapComments.class);
        job5.setReducerClass(ReduceSort.class);
        job5.setMapOutputKeyClass(LongWritable.class);
        job5.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job5, new Path(args[1] + "Inter_2"));
        FileOutputFormat.setOutputPath(job5, new Path(args[1] + "commentStats"));


        boolean status = job5.waitForCompletion(true);
        if (status == true) {
            end = new Date().getTime();
            System.out.println("\nJob took " + (end - start) + "milliseconds\n");
            System.out.println("\nParsing took " + (end1 - start) + "milliseconds\n");
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
                            int catId = Integer.parseInt(nextLine[4].trim());
                            long viewCount = Long.parseLong(nextLine[7].trim());
                            long likeCount = Long.parseLong(nextLine[8].trim());
                            long dislikeCount = Long.parseLong(nextLine[9].trim());
                            long commentCount = Long.parseLong(nextLine[10].trim());
                            if (vidURL.equals("#NAME?") || vidURL.equals("#VALUE!")) { //for URL N/A, id ny vid name
                                currentKey = new Text(vidName);
                                currentVal = new Text(vidURL + "\t" + viewCount + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount + "\t" + catId);
                            } else { //for url present, id by url for removing rudundant vids of changed titlenames
                                currentKey = new Text(vidURL);
                                currentVal = new Text(vidName + "\t" + viewCount + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount + "\t" + catId);

                            }
                            context.write(currentKey, currentVal);
                        } else {
                            throw new Exception(); //avoid ds related exceptions and remove 1st row
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
            String cVal, stat = "", catId = "", token2;
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
                    catId = nextLine[5];
                }
                reader.close();
            }


            stat = catId + "\t" + stat;
            newWord = new Text(stat);
            context.write(newWord, new LongWritable(max)); //stat and views as value

        }
    }

    public static class MapCat extends Mapper < LongWritable, Text, Text, Text > {

        private String line,
        catName,
        viewCount,
        likeCount,
        dislikeCount,
        commentCount;
        String[] nextLine;
        Text currentKey,
        currentVal;
        CSVReader reader;

        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            line = lineText.toString();
            if (line.length() > 0) {
                reader = new CSVReader(new StringReader(line), '\t', '"', 0);
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 5 && !(nextLine[0].trim().equals("video_id"))) {
                            catName = nextLine[0].trim();
                            viewCount = nextLine[4].trim();
                            likeCount = nextLine[1].trim();
                            dislikeCount = nextLine[2].trim();
                            commentCount = nextLine[3].trim();
                            currentKey = new Text(catName);
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

    public static class ReduceCat extends Reducer < Text, Text, Text, Text > {
        private String catId,
        catName,
        sumStats;
        private CSVReader reader;
        private long comments,
        likes,
        dislikes,
        views;
        @Override
        public void reduce(Text word, Iterable < Text > vals, Context context)
        throws IOException,
        InterruptedException {
            comments = 0;
            likes = 0;
            dislikes = 0;
            views = 0;
            catId = word.toString().trim();
            for (Text val: vals) {
                reader = new CSVReader(new StringReader(val.toString().trim()), '\t', '"', 0);
                String[] nextLine = reader.readNext();
                likes += Long.parseLong(nextLine[0]);
                dislikes += Long.parseLong(nextLine[1]);
                comments += Long.parseLong(nextLine[2]);
                views += Long.parseLong(nextLine[3]);
                reader.close();

            }
            if ((catName = hmap.get(Integer.parseInt(catId))) == null)
                catName = "" + region + "_" + catId; //for categnotfound
            sumStats = likes + "\t" + dislikes + "\t" + comments + "\t" + views;
            context.write(new Text(catName), new Text(sumStats));
        }
    }


    public static class MapViews extends Mapper < LongWritable, Text, LongWritable, Text > {


        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {

            String line = lineText.toString();
            String catName, viewCount, likeCount, dislikeCount, commentCount;
            String[] nextLine;
            Text currentVal;
            CSVReader reader;
            LongWritable currentKey;

            if (line.length() > 0) {
                reader = new CSVReader(new StringReader(line), '\t', '"', 0);
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 5 && !(nextLine[0].trim().equals("video_id"))) {
                            catName = nextLine[0].trim();
                            viewCount = nextLine[4].trim();
                            likeCount = nextLine[1].trim();
                            dislikeCount = nextLine[2].trim();
                            commentCount = nextLine[3].trim();
                            currentKey = new LongWritable(Long.parseLong(viewCount) * -1);
                            currentVal = new Text(catName + "\t" + likeCount + "\t" + dislikeCount + "\t" + commentCount);
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

        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            String catName, viewCount, likeCount, dislikeCount, commentCount;
            String[] nextLine;
            Text currentVal;
            CSVReader reader;
            LongWritable currentKey;
            if (line.length() > 0) {
                reader = new CSVReader(new StringReader(line), '\t', '"', 0);

                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 5 && !(nextLine[0].trim().equals("video_id"))) {
                            catName = nextLine[0].trim();
                            viewCount = nextLine[4].trim();
                            likeCount = nextLine[1].trim();
                            dislikeCount = nextLine[2].trim();
                            commentCount = nextLine[3].trim();
                            currentKey = new LongWritable(Long.parseLong(likeCount) * -1);
                            currentVal = new Text(catName + "\t" + viewCount + "\t" + dislikeCount + "\t" + commentCount);
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

        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            String catName, viewCount, likeCount, dislikeCount, commentCount;
            String[] nextLine;
            Text currentVal;
            CSVReader reader;
            LongWritable currentKey;

            if (line.length() > 0) {
                reader = new CSVReader(new StringReader(line), '\t', '"', 0);

                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 5 && !(nextLine[0].trim().equals("video_id"))) {
                            catName = nextLine[0].trim();
                            viewCount = nextLine[4].trim();
                            likeCount = nextLine[1].trim();
                            dislikeCount = nextLine[2].trim();
                            commentCount = nextLine[3].trim();
                            currentKey = new LongWritable(Long.parseLong(dislikeCount) * -1);
                            currentVal = new Text(catName + "\t" + viewCount + "\t" + likeCount + "\t" + commentCount);
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

        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            String catName, viewCount, likeCount, dislikeCount, commentCount;
            String[] nextLine;
            Text currentVal;
            CSVReader reader;
            LongWritable currentKey;
            if (line.length() > 0) {
                reader = new CSVReader(new StringReader(line), '\t', '"', 0);
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 5 && !(nextLine[0].trim().equals("video_id"))) {
                            catName = nextLine[0].trim();
                            viewCount = nextLine[4].trim();
                            likeCount = nextLine[1].trim();
                            dislikeCount = nextLine[2].trim();
                            commentCount = nextLine[3].trim();
                            currentKey = new LongWritable(Long.parseLong(commentCount) * -1);
                            //viewInteger=Long.parseLong(nextLine[7].trim());
                            //views = new LongWritable(viewInteger);
                            currentVal = new Text(catName + "\t" + viewCount + "\t" + likeCount + "\t" + dislikeCount);
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