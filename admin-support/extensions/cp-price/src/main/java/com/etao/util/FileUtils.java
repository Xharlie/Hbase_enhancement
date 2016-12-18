/**
 * 
 */
package com.etao.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * @author dihong.wq
 * 
 */
public class FileUtils {
	private static Log LOG = LogFactory.getLog(FileUtils.class);

	public static String readContentFromLocal(String localPath) {
		String content = "";

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(localPath));
			String line = null;
			while ((line = reader.readLine()) != null) {
				content += line;
			}
		} catch (IOException ioe) {
			LOG.warn("failed to read local file '" + localPath + "'", ioe);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}

		return content;
	}

	public static String readContentFromHdfs(Configuration conf, String hdfsPath) {
		String jsonStr = "";
		FSDataInputStream in = null;
		try {
			FileSystem fs = FileSystem.get(conf);

			Path path = new Path(hdfsPath);
			in = fs.open(path);

			LineReader lineReader = new LineReader(in);
			Text line = new Text();
			while (lineReader.readLine(line) > 0) {
				jsonStr += line.toString();
			}
		} catch (IOException ioe) {
			LOG.warn("failed to read json file '" + hdfsPath + "'", ioe);
			jsonStr = "";
		} finally {
			IOUtils.closeStream(in);
		}

		return jsonStr;
	}

	public static List<String> readContentsFromHdfs(Configuration conf,
			String hdfsPath) {
		List<String> contents = new ArrayList<String>();
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(hdfsPath);
			FileStatus[] fileStatuses = fs.listStatus(path, new PathFilter() {
				@Override
				public boolean accept(Path p) {
					boolean ret = false;
					if (p.getName().endsWith(".json")) {
						ret = true;
					}
					return ret;
				}
			});

			if (fileStatuses != null) {
				for (FileStatus fileStatus : fileStatuses) {
					String content = readContentFromHdfsPath(fs,
							fileStatus.getPath());
					if (content == null || content.isEmpty()) {
						continue;
					}
					contents.add(content);
				}
				if (fileStatuses.length != contents.size()) {
					LOG.warn("there're " + fileStatuses.length
							+ " rule file(s), but only parsed "
							+ contents.size() + " rule(s)");
					return null;
				}
			}

		} catch (IOException ioe) {
			LOG.warn("failed to list file(s) in the directory '" + hdfsPath
					+ "'", ioe);
		} finally {
		}

		return contents;
	}

	private static String readContentFromHdfsPath(FileSystem fs, Path path) {
		String content = "";
		FSDataInputStream in = null;
		try {
			in = fs.open(path);

			LineReader lineReader = new LineReader(in);
			Text line = new Text();
			while (lineReader.readLine(line) > 0) {
				content += line.toString();
			}
		} catch (IOException ioe) {
			LOG.warn("failed to read file '" + path.toUri() + "'", ioe);
			content = "";
		} finally {
			IOUtils.closeStream(in);
		}

		return content;
	}
}
