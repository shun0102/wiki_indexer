package net.shun;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import net.shun.mapreduce.lib.input.XmlInputFormat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiIndexer {
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text title = new Text();
    //private Text content = new Text();
    private Text word = new Text();
    private int N = 2;

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      try {
        String valueStr = value.toString();
        Pattern p = Pattern.compile("<title([^>]*)>([^<]*)</title>");
        Matcher m = p.matcher( valueStr );
        m.find();
        title.set(m.group( 2 ));

        p = Pattern.compile("<text([^>]*)>([^<]*)</text>");
        m = p.matcher( valueStr );
        m.find();

        String text = m.group( 2 );
        String token = new String();

        StringTokenizer tokenizer = new StringTokenizer(text);
        while (tokenizer.hasMoreTokens()) {
          token = tokenizer.nextToken();
          token = token.replaceAll("[!#%&'@;:,./「」。、]", "");
          token = token.replaceAll("[\\*\\+\\.\\?\\{\\}\\(\\)\\[\\]\\^\\$\\-\\|]", "");
          token = token.replaceAll("[0-9]", "");
          for (int i = 0; i < token.length() - 1; i++) {
            if (i + N < token.length()) {
              word.set(token.substring(i, i + N));
              word.set(token);
              context.write(word, title);
            }
          }
        }
      } catch(IllegalStateException ex) {
        System.err.println(ex);
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    Text titles = new Text();
    String title;

    protected void reduce(Text key, Iterable<Text> values, 
		       Context context) throws IOException, InterruptedException {

      String output = new String();
      HashSet written = new HashSet();

      for (Text value : values) {
        title = value.toString();
        if (! written.contains( title )){
          written.add( title );
          output = output + title + " ";
        }
      }

      if (output.length() != 0) {
        titles.set(output);
        context.write(key, titles);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    args = parser.getRemainingArgs();

    Job job = new Job(conf, "wikiindexer");
    job.setJarByClass(WikiIndexer.class);


    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormatClass(XmlInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    boolean success = job.waitForCompletion(true);
    System.out.println(success);
  }
}
