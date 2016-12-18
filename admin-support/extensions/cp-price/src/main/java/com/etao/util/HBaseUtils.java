/**
 * 
 */
package com.etao.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author dihong.wq
 * 
 */
public class HBaseUtils {
	public static boolean containsColumn(Mutation mutation, String column) {
		boolean ret = false;
		// case for 'family:qualifier'
		if (column.indexOf(KeyValue.COLUMN_FAMILY_DELIMITER) > 0) {
			byte[][] columnBytes = KeyValue.parseColumn(Bytes.toBytes(column));
			Pattern qualifierPattern = Pattern.compile(Bytes
					.toString(columnBytes[1]));

			Map<byte[], List<KeyValue>> familyMap = mutation.getFamilyMap();
			List<KeyValue> kvList = familyMap.get(columnBytes[0]);
			if (kvList != null && kvList.size() > 0) {
				for (KeyValue kv : kvList) {
					String qualifier = Bytes.toString(kv.getBuffer(),
							kv.getQualifierOffset(), kv.getQualifierLength());
					if (qualifierPattern.matcher(qualifier).matches()) {
						ret = true;
						break;
					}
				}
			}
		} else {// case for 'family' only
			ret = mutation.getFamilyMap().containsKey(Bytes.toBytes(column));
		}

		return ret;
	}

	public static List<KeyValue> getColumns(Mutation mutation, String column) {
		List<KeyValue> kvList = new ArrayList<KeyValue>();

		String family = column;
		String qualifier = null;
		// case for 'family:qualifier'
		if (column.indexOf(KeyValue.COLUMN_FAMILY_DELIMITER) > 0) {
			byte[][] columnBytes = KeyValue.parseColumn(Bytes.toBytes(column));
			family = Bytes.toString(columnBytes[0]);
			qualifier = Bytes.toString(columnBytes[1]);
		}

		Map<byte[], List<KeyValue>> familyMap = mutation.getFamilyMap();
		if (qualifier == null) {
			kvList = familyMap.get(Bytes.toBytes(family));
		} else {
			List<KeyValue> familyKvList = familyMap.get(Bytes.toBytes(family));
			if (familyKvList != null && familyKvList.size() > 0) {
				for (KeyValue kv : familyKvList) {
					if (Pattern
							.compile(qualifier)
							.matcher(
									Bytes.toString(kv.getBuffer(),
											kv.getQualifierOffset(),
											kv.getQualifierLength())).matches()) {
						kvList.add(kv);
					}
				}
			}
		}

		return kvList;
	}

	public static KeyValue getColumnLatest(Mutation mutation, String family,
			String qualifier) {
		KeyValue kv = null;

		List<KeyValue> kvList = getColumns(mutation, family
				+ KeyValue.COLUMN_FAMILY_DELIMITER + qualifier);
		if (kvList != null && kvList.size() > 0) {
			kv = kvList.get(0);
		}

		return kv;
	}

	public static String toJSON(Operation operation) {
		String json = "{}";
		try {
			json = operation == null ? "{}" : operation.toJSON();
		} catch (IOException e) {
		}

		return json;
	}
}
