import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.*;

/**
 * Created by haiki on 3/27/17.
 */
public class NGramLibraryBuilder {
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        int noGram;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            noGram = conf.getInt("noGram", 5);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", "");
            String[] words = line.split("\\s+");
            if(words.length < 2){
                return;
            }
            StringBuilder sb;
            for(int i = 0; i < words.length; i++){
                sb = new StringBuilder();
                sb.append(words[i]);
                for(int j = 1; j + i < words.length && j < noGram; j++){
                    sb.append("");
                    sb.append(words[i + j]);
                    context.write(new Text(sb.toString()), new IntWritable(1));
                }
            }

        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}