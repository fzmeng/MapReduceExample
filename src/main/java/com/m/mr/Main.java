package com.m.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 操作person.csv文件
 */
public class Main extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AgePartition <input> <output>");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(2);
        }

        Configuration conf = getConf();
        //conf.set(RegexMapper.GROUP,"female");
        Job job = Job.getInstance(conf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        job.setJarByClass(Main.class);
        job.setMapperClass(DefinedMap.class);
        //设置map的输出key和value类型
        job.setMapOutputKeyClass(DefinedCombinationKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(DefinedReducer.class);
        //设置reduce的输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //自定义分区策略
        job.setPartitionerClass(DefinedPartitioner.class);
        //自定义排序策略，在自定义组合键重写方法compareTo时若自定义排序策略与之相同可以省略自定义排序策略。最终结果以自定义排序策略为主
        job.setSortComparatorClass(DefinedSort.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(3);//reducer num  = partition num

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }
}


