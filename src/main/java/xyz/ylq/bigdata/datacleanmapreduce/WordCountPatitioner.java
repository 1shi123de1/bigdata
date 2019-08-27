package xyz.ylq.bigdata.datacleanmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPatitioner extends Partitioner<Text, IntWritable> {

	// 按照 单词 ASC 码奇偶分区
	@Override
	public int getPartition(Text arg0, IntWritable arg1, int arg2) {
		String firWord = arg0.toString().substring(0, 1);
		char[] charArray = firWord.toCharArray();
		int result = charArray[0];
		
		if(result %2 == 0){
			return 0;
		}else{
			return 1;
		}
		
	}

}
