import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by haiki on 3/29/17.
 */
public class Driver {
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
        String inputDir = args[0];
        String outputDir = args[1];
        String noGram = args[2];
        String threshold = args[3];
        String topK = args[4];

        Configuration configuration1 = new Configuration();
        configuration1.set("textinputformat.record.delimiter", ".");
        configuration1.set("noGram", noGram);

        Job job1 = Job.getInstance();
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(outputDir));

        job1.waitForCompletion(true);

        Configuration configuration2 = new Configuration();
        configuration2.set("threshold", threshold);
        configuration2.set("topK", topK);

        DBConfiguration.configureDB(configuration2,
                "com.mysql.jdbc.Drive",
                "jdbc:mysql://ip_address:port/test",
                "root",
                "password");

        Job job2 = Job.getInstance();
        job2.setJobName("LanguageModel");
        job2.setJarByClass(Driver.class);

        job2.setMapperClass(LanguageModel.Map.class);
        job2.setReducerClass(LanguageModel.Reduce.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job2, new Path(inputDir));
        TextOutputFormat.setOutputPath(job2, new Path(outputDir));
    }
}
