package com.decre.hadoop.hadoopbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/5 0005 20:10
 * @since 1.0.0
 * Descirption: 自定义wordcount的MapReduce类
 */
public class WordCountMR {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // TODO 怎么动态读取configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.0.125:9000");
        // 通过conf获取job对象，该对象会组织所有的该MapReduce程序的所有组件
        Job job = Job.getInstance(conf);

        // 设置jar包所在的路径
        job.setJarByClass(WordCountMR.class);

        // 指定mapper类和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 假如 mapTask的输出key-value类型，跟reduceTask的输出key-value类型一致，那么，以上两句代码可以不用设置
        // reduceTask的输入key-value类型 就是 mapTask的输出key-value类型。所以不需要指定
        // 指定reducetask的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 为job指定输入数据的组件和输出数据的组件，以下两个参数是默认的，所以不指定也是OK的
        // job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);

        // 为该mapreduce程序制定默认的数据分区组件。默认是 HashPartitioner.class
        // job.setPartitionerClass(HashPartitioner.class);

        // 如果MapReduce程序在Eclipse中，运行，也可以读取Windows系统本地的文件系统中的数据
        Path inputPath = new Path(generateHdfsPath("/testDir"));
        Path outputPath = new Path(generateHdfsPath("/output1"));

        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        // 设置wordcount程序的输入路径
        FileInputFormat.setInputPaths(job, inputPath);
        // 设置wordcount程序的输出路径
        FileOutputFormat.setOutputPath(job, outputPath);

        // job.submit()
        // 最后提交任务（verbose布尔值 决定要不要将运行进度信息输出给用户
        boolean waitForeCompletion = job.waitForCompletion(true);
        System.exit(waitForeCompletion ? 0 : 1);
    }



    /**
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     * <p>
     * KEYIN 是指框架读取到的数据的key的类型，在默认的InputFormat下，读到的key是一行文本的起始偏移量，所以key的类型是Long
     * VALUEIN 是指框架读取到的数据的value的类型,在默认的InputFormat下，读到的value是一行文本的内容，所以value的类型是String
     * KEYOUT 是指用户自定义逻辑方法返回的数据中key的类型，由用户业务逻辑决定，在此wordcount程序中，我们输出的key是单词，所以是String
     * VALUEOUT 是指用户自定义逻辑方法返回的数据中value的类型，由用户业务逻辑决定,在此wordcount程序中，我们输出的value是单词的数量，所以是Integer
     * <p>
     * 但是，String ，Long等jdk中自带的数据类型，在序列化时，效率比较低，hadoop为了提高序列化效率，自定义了一套序列化框架
     * 所以，在hadoop的程序中，如果该数据需要进行序列化（写磁盘，或者网络传输），就一定要用实现了hadoop序列化框架的数据类型
     * <p>
     * Long ----> LongWritable
     * String ----> Text
     * Integer ----> IntWritable
     * Null ----> NullWritable
     */
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        /**
         * LongWritable key : 该key就是value该行文本的在文件当中的起始偏移量
         * Text value ： 就是MapReduce框架默认的数据读取组件TextInputFormat读取文件当中的一行文本
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable());
            }
        }
    }

    /**
     * 首先，和前面一样，Reducer类也有输入和输出，输入就是Map阶段的处理结果，输出就是Reduce最后的输出
     * reducetask在调我们写的reduce方法,reducetask应该收到了前一阶段（map阶段）中所有maptask输出的数据中的一部分
     * （数据的key.hashcode%reducetask数==本reductask号），所以reducetaks的输入类型必须和maptask的输出类型一样
     * <p>
     * reducetask将这些收到kv数据拿来处理时，是这样调用我们的reduce方法的： 先将自己收到的所有的kv对按照k分组（根据k是否相同）
     * 将某一组kv中的第一个kv中的k传给reduce方法的key变量，把这一组kv中所有的v用一个迭代器传给reduce方法的变量values
     */
    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Text key : mapTask输出的key值
         * Iterable<IntWritable> values ： key对应的value的集合（该key只是相同的一个key）
         * reduce方法接收key值相同的一组key-value进行汇总计算
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // 结果汇总
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            // 汇总结果往外输出
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * 将相对路径转化为HDFS文件路径
     *
     * @param dstPath 相对路径，比如：/data
     * @return java.lang.String
     * @author decre
     * @since 1.0.0
     */
    private  static String generateHdfsPath(String dstPath) {
        String hdfsPath = "hdfs://192.168.0.125:9000";
        if (dstPath.startsWith("/")) {
            hdfsPath += dstPath;
        } else {
            hdfsPath = hdfsPath + "/" + dstPath;
        }

        return hdfsPath;
    }

}
