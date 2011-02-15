/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.shun.mapreduce.lib.input;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.DataOutputBuffer;


/**
 * Treats keys as offset in file and value as line. 
 */
public class XmlRecordReader extends RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(XmlRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  //private LineReader in;
  private BufferedInputStream in;
  private int maxLineLength;
  private LongWritable key = null;
  private Text value = null;
  private String beginMark;
  private String endMark;

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    String[] beginMarks = job.getStrings("mapred.xmlrecordreader.begin", "<page>");
    this.beginMark = beginMarks[0];
    String[] endMarks = job.getStrings("mapred.xmlrecordreader.begin", "</page>");
    this.endMark = endMarks[0];
    
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    fileIn.seek(start);
    in = new BufferedInputStream(fileIn); 
    /*
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    */
    this.pos = start;
    readUntilMatch(beginMark, false, null);
  }

  boolean readUntilMatch(String textPat, boolean includePat, DataOutputBuffer outBufOrNull) throws IOException {
    byte[] cpat = textPat.getBytes("UTF-8");
    int m = 0;
    boolean match = false;
    int msup = cpat.length;
    int LL = 120000 * 10;
    
    in.mark(LL);
    while (true) {
      int b = in.read();
      if (b == -1) break;

      byte c = (byte) b;
      if (c == cpat[m]) {
        m++;
        if (m == msup) {
	  match = true;
	  break;
	}
      } else {
	in.mark(LL);
	if (outBufOrNull != null) {
	  outBufOrNull.write(cpat, 0, m);
	  outBufOrNull.write(c);
	  pos += m;
	}
	m = 0;
      }
    }
    if (!includePat && match) {
      in.reset();
    } else if (outBufOrNull != null) {
      outBufOrNull.write(cpat);
      pos += msup;
    }
    return match;
  }
  
  public boolean nextKeyValue() throws IOException {
    if (key == null)
      key = new LongWritable();
    key.set(pos);
    if (value == null)
      value = new Text();

    if (pos >= end)
      return false;

    DataOutputBuffer buf = new DataOutputBuffer();
    //read until begin
    if (!readUntilMatch(beginMark, false, null)) {
      return false;
    }
    //read until end
    if (!readUntilMatch(endMark, true, buf)) {
      return false;
    }

    byte[] record = new byte[buf.getLength()];
    System.arraycopy(buf.getData(), 0, record, 0, record.length);
    value.set(record);

    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}
