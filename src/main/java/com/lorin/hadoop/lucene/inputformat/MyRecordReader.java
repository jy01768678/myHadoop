package com.lorin.hadoop.lucene.inputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyRecordReader extends RecordReader<Text, Text> {

	private CombineFileSplit combineFileSplit; // ��ǰ����ķ�Ƭ
	private int totalLength;                   // ��Ƭ����ļ�����
	private int currentIndex;                  // ��ǰ������ļ�����
	private float currentProgress = 0;         // ��ǰ�Ľ��
	private Text currentKey = new Text();      // ��ǰ��Key
	private Text currentValue = new Text();    // ��ǰ��Value
	private Configuration conf;                // ������Ϣ
	private boolean processed;                 // ��¼��ǰ�ļ��Ƿ��Ѿ���ȡ

	public MyRecordReader(CombineFileSplit combineFileSplit,
			TaskAttemptContext context, Integer index) throws IOException {
		super();
		this.currentIndex = index;
		this.combineFileSplit = combineFileSplit;
		conf = context.getConfiguration();
		totalLength = combineFileSplit.getPaths().length;
		processed = false;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException {
		if (currentIndex >= 0 && currentIndex < totalLength) {
			currentProgress = (float) currentIndex / totalLength;
			return currentProgress;
		}
		return currentProgress;
	}

	@Override
	public void close() throws IOException {
	}
	
	@Override
	public boolean nextKeyValue() throws IOException {
		if (!processed) {    // ����ļ�δ�������ȡ�ļ�������key-value
			// set key
			Path file = combineFileSplit.getPath(currentIndex);
			currentKey.set(file.getParent().getName()); // category's name
			// set value
			FSDataInputStream in = null;
			byte[] contents = new byte[(int)combineFileSplit.getLength(currentIndex)];
			try {
				FileSystem fs = file.getFileSystem(conf);
				in = fs.open(file);
				in.readFully(contents);
				currentValue.set(contents);
			} catch (Exception e) {
			} finally {
				in.close();
			}
			processed = true;
			return true;
		}
		return false;        //����ļ��Ѿ����?���뷵��false
	}
	
}