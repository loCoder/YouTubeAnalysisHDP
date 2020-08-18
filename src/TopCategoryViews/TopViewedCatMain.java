package org.myorg;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.StringReader;
import com.opencsv.CSVReader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class TopViewedCatMain extends Configured implements Tool {
    public static String[] regionName = {
        "CA",
        "DE",
        "FR",
        "GB",
        "IN",
        "JP",
        "KR",
        "MX",
        "RU",
        "US"
    };
    public static HashMap < Integer, String > hmap;
    private static long start, end;
    public static List < HashMap < Integer, String >> hmapList = new ArrayList < HashMap < Integer, String >> ();
    public static void main(String[] args) throws Exception 
    {
        start = new Date().getTime();
        int j = 0;
        while (j < 10) 
	{
            HashMap < Integer, String > hmap1 = new HashMap < Integer, String > ();
            FileReader fr = new FileReader("/home/woir/YTA/JAVA/json/" + regionName[j] + "_category_id.json");
            Object obj = new JSONParser().parse(fr);
            JSONObject jo = (JSONObject) obj;
            JSONArray items = (JSONArray) jo.get("items");
            Iterator i = items.iterator();
            while (i.hasNext()) 
	    {
                JSONObject innerObj = (JSONObject) i.next();
                JSONObject structure = (JSONObject) innerObj.get("snippet");
                hmap1.put(Integer.parseInt((String) innerObj.get("id")), (String) structure.get("title"));
            }
            hmapList.add(j, hmap1);
            j++;
            fr.close();
        }
        int res = ToolRunner.run(new TopViewedCatMain(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception 
    {
        Job job = Job.getInstance(getConf(), "trendcat1");
        Job job2 = Job.getInstance(getConf(), "trendcat2");

        job.setJarByClass(TopViewedCatMain.class);
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"Inter_1"));
        job.waitForCompletion(true);
        end = new Date().getTime();
        System.out.println("\nJob 1 took " + (end - start) + "milliseconds\n");

        job2.setJarByClass(TopViewedCatMain.class);
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]+"Inter_1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"catViews"));
        boolean status = job2.waitForCompletion(true);




        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        /*FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        boolean status = job.waitForCompletion(true);*/
        if (status == true) 
	{
            end = new Date().getTime();
            System.out.println("\nJob took " + (end - start) + "milliseconds\n");
            return 0;
        } 
	else
            return 1;
    }

    public static class Map1 extends Mapper < LongWritable, Text, Text, LongWritable > 
    {
        private LongWritable views = new LongWritable();
        private Text word = new Text();
        private long viewInteger;
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException 
	{
            String line = lineText.toString();
            Text currentWord = new Text();
            if (line.length() > 0) 
	    {
                CSVReader reader = new CSVReader(new StringReader(line));
                String[] nextLine;
                try 
		{
                    if ((nextLine = reader.readNext()) != null) 
		    {
                        if (nextLine.length == 16) 
			{
                            FileSplit fileSplit = (FileSplit) context.getInputSplit();
                            String filename = fileSplit.getPath().getName();
                            String regId = filename.substring(0, 2);
                            String vidURL = nextLine[0].trim();
                            String vidName = nextLine[2].trim();
                            String cat_id = nextLine[4].trim();
                            currentWord = new Text(vidURL + "\t" + vidName + "\t" + regId + "\t" + cat_id);
                            viewInteger = Long.parseLong(nextLine[7].trim());
                            views = new LongWritable(viewInteger);
                            context.write(currentWord, views);
                        } 
			else 
                            throw new Exception();//caution - use a user defined exception instead 
                    }
		} 
		catch (NumberFormatException e) {} 
		catch (NoClassDefFoundError e) {} 
		catch (Exception e) {} //handle it for further use
		finally 
		{
                    reader.close();
                } //close for each line
            }
        }
    }


    public static class Reduce1 extends Reducer < Text, LongWritable, Text, LongWritable > {
        @Override
        public void reduce(Text word, Iterable < LongWritable > counts, Context context)
        throws IOException,
        InterruptedException {
            long max = 0;
            long ccount;
            for (LongWritable count: counts) {
                ccount = count.get();
                max = (max > ccount) ? max : ccount;
            }
            String[] tokens = word.toString().trim().split("\t");
            Text newWord = new Text(tokens[2] + "\t" + tokens[3]);
            context.write(newWord, new LongWritable(max));
        }
    }



    public static class Map2 extends Mapper < LongWritable, Text, Text, LongWritable > {
        private LongWritable views = new LongWritable();
        private Text word = new Text();
        private long numRecords = 0;
        private String category_id;
        private long viewInteger;
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,
        InterruptedException {
            String line = lineText.toString();
            Text currentWord = new Text();
            if (line.length() > 0) {
                CSVReader reader = new CSVReader(new StringReader(line), '\t', '"', 0);
                String[] nextLine;
                try {
                    if ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length == 3) {
                            int i = 0;
                            String regId = nextLine[0].trim();
                            category_id = nextLine[1].trim();
                            String catName;
                            while (i < regionName.length) {
                                if (regionName[i].equals(regId.trim()))
                                    break;
                                i++;
                            }
                            if (i >= regionName.length)
                                currentWord = new Text("" + regId + "_" + category_id); //for jsonnotfound
                            else {
                                if ((catName = hmapList.get(i).get(Integer.parseInt(category_id))) == null)
                                    currentWord = new Text("" + regId + "_" + category_id); //for categnotfound
                                else
                                    currentWord = new Text(catName);
                            }
                            viewInteger = Long.parseLong(nextLine[2].trim());
                            views = new LongWritable(viewInteger);
                            context.write(currentWord, views);
                        } else {
                            throw new Exception(); //use carefully, instead use expected exceptions, or a userdef exception for insufficientcolumns
                        }

                    }
                } catch (NumberFormatException e) {} catch (NoClassDefFoundError e) {} catch (Exception e) {} finally {
                    reader.close();
                } //for single line
            }
        }
    }
    public static class Reduce2 extends Reducer < Text, LongWritable, Text, LongWritable > {
        @Override
        public void reduce(Text word, Iterable < LongWritable > counts, Context context)
        throws IOException,
        InterruptedException {
            long sum = 0;
            long ccount;
            Text newWord;
            String cat_str = word.toString();
            int category_id;
            for (LongWritable count: counts) {
                ccount = count.get();
                sum += ccount;
            }

            newWord = word;
            context.write(newWord, new LongWritable(sum));
        }
    }
}