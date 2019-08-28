package xyz.ylq.bigdata.datacleanmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountRunner {

	public static void main(String[] args) throws Exception {
		// 参数长度判断
		if(args == null ||args.length<2){
			System.out.println("parameter length error!!");
			return;
		}
		Path outPath = new Path(args[args.length-1]);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCountRunner");
		job.setJarByClass(xyz.ylq.bigdata.datacleanmapreduce.WordCountRunner.class);
		job.setMapperClass(xyz.ylq.bigdata.datacleanmapreduce.WordCountMap.class);

		job.setReducerClass(xyz.ylq.bigdata.datacleanmapreduce.WordCountReduce.class);

		// 分区需要，指定分区类，reduce数为2
//		job.setPartitionerClass(WordCountPatitioner.class);
//		job.setNumReduceTasks(2);
//		
//		// 指定combiner类
//		job.setCombinerClass(WordCountReduce.class);
//		
//		// 设置InputFormat，处理小文件时会合并到指定的大小再处理
//		job.setInputFormatClass(CombineTextInputFormat.class);
//		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
		
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		// 多个input目录
		for(int i = 0; i < args.length-1; i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		// 删除output Path目录
		outPath.getFileSystem(conf).delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}
