/**
 * 
 */
package com.etao.hbase.coprocessor.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * @author yutian.xb
 * 
 */
public class FileUtils {
    private static Log LOG = LogFactory.getLog(FileUtils.class);

    public static String readContentFromLocal(String localPath) {
	StringBuffer contentBuffer = new StringBuffer();
	BufferedReader reader = null;
	try {
	    reader = new BufferedReader(new FileReader(localPath));
	    String line = null;
	    while (null != (line = reader.readLine())) {
		contentBuffer.append(line);
	    }
	} catch (IOException ioe) {
	    LOG.warn("failed to read local file '" + localPath + "'", ioe);
	} finally {
	    try {
		if (null != reader) {
		    reader.close();
		}
	    } catch (IOException e) {
	    }
	}

	return contentBuffer.toString();
    }

    public static String readContentFromHdfs(Configuration conf, String hdfsPath) {
	StringBuffer contentBuffer = new StringBuffer();
	FSDataInputStream in = null;
	try {
	    FileSystem fs = FileSystem.get(conf);

	    Path path = new Path(hdfsPath);
	    in = fs.open(path);

	    LineReader lineReader = new LineReader(in);
	    Text line = new Text();
	    while (lineReader.readLine(line) > 0) {
		contentBuffer.append(line.toString());
	    }
	} catch (IOException ioe) {
	    LOG.warn("failed to read json file '" + hdfsPath + "'", ioe);
	    contentBuffer.setLength(0);
	} finally {
	    IOUtils.closeStream(in);
	}

	return contentBuffer.toString();
    }
}
