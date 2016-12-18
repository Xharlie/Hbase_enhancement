<%--
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
--%>
<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.HashMap"
  import="org.apache.commons.lang.StringEscapeUtils"
  import="org.apache.hadoop.io.Writable"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.HTable"
  import="org.apache.hadoop.hbase.client.Result"
  import="org.apache.hadoop.hbase.client.Get"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.HTableDescriptor"
  import="org.apache.hadoop.hbase.HColumnDescriptor"
  import="org.apache.hadoop.hbase.client.HConnectionManager"
  import="org.apache.hadoop.hbase.io.ImmutableBytesWritable"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.util.FSUtils"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.filter.Filter"
  import="org.apache.hadoop.hbase.filter.ColumnCountGetFilter"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="java.text.SimpleDateFormat"
  import="java.util.*"
  import="org.apache.hadoop.hbase.HConstants"%><%
  
  class WebUtil{
    public  String getReverseURL(String url) {
		int first_stop = url.indexOf("://");
		if (first_stop == -1)
			return url;
		int second_stop = url.indexOf("/", first_stop + 3);
		if (second_stop == -1)
			second_stop = url.length();
		String header = url.substring(0, first_stop + 3);
		String tail = url.substring(second_stop);
		StringBuffer sb = new StringBuffer(url.subSequence(first_stop + 3,
				second_stop));
		sb = sb.reverse();
		StringBuffer host = new StringBuffer();
		int start = 0;
		int stop = 0;
		while ((stop = sb.indexOf(".", start)) != -1) {
			StringBuffer tmp_sb = new StringBuffer(sb.substring(start, stop));
			tmp_sb = tmp_sb.reverse();
			start = stop + 1;
			host.append(tmp_sb).append(".");
		}
		StringBuffer tmp_sb = new StringBuffer(sb.substring(start));
		tmp_sb = tmp_sb.reverse();
		host.append(tmp_sb);
		StringBuffer re = new StringBuffer();
		re.append(header).append(host).append(tail);
		return re.toString();
	}
  }
  WebUtil webUtilObj = new WebUtil();
  
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  HBaseAdmin admin = new HBaseAdmin(conf);
  String rowKey = request.getParameter("key");
  String tableName = request.getParameter("name");
  String maxVersions = request.getParameter("max_versions");
  String[] families = request.getParameterValues("families");
  String rowkeyType = request.getParameter("rowkey_type");
  String reversedUrlChecked = "";
  String urlChecked = "";
  String queryKey = "";
  ArrayList<String> familyList = new ArrayList<String>();
  if(null != families){
    for(String family : families){
      familyList.add(family);
    }
  }
  HTable table = new HTable(conf, tableName);
  if(rowKey == null){
    rowKey = "";
  }
  else{
    rowKey = rowKey.trim();
    rowKey = StringEscapeUtils.unescapeHtml(rowKey);
  }
  if(maxVersions == null){
    maxVersions = "1";
  }
  else{
    maxVersions = maxVersions.trim();
  }
  if(rowkeyType!=null && rowkeyType.equalsIgnoreCase("url")){
     urlChecked = "checked";
     queryKey = webUtilObj.getReverseURL(rowKey);
  }
  else{
     reversedUrlChecked = "checked";
     queryKey = rowKey;
  }
  boolean displayRowkeyType = false;
  String[] tablesByURLQuery = conf.getStrings("hbase.master.web.tablequery.byurl.tables");
  if(tablesByURLQuery!=null){
    for(String tableByURLQuery : tablesByURLQuery){
      if(tableByURLQuery.equalsIgnoreCase(tableName)){
        displayRowkeyType = true;
        break;
      }
    }
  }
 
  Result result = new Result();
  if ( rowKey != null && !queryKey.isEmpty()){
    Get get = new Get(queryKey.getBytes());
    if(null != families){
      for(String family : families){
        get.addFamily(Bytes.toBytes(family));
      }
    }
    get.setMaxVersions(Integer.valueOf(maxVersions));
    result = table.get(get);
    table.close();
  }
%>

<?xml version="1.0" encoding="UTF-8" ?>
<!-- Commenting out DOCTYPE so our blue outline shows on hadoop 0.20.205.0, etc.
     See tail of HBASE-2110 for explaination.
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
-->
<html xmlns="http://www.w3.org/1999/xhtml">

<head>
	<title>HBase Table Query Tool</title>
	<meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<meta name="description" content="">
	<meta name="author" content="">
	<link href="/static/css/bootstrap.min.css" rel="stylesheet">
	<link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
	<link href="/static/css/hbase.css" rel="stylesheet">
</head>

<body>
<!--<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo.png" alt="HBase Logo" title="HBase Logo" /></a>-->
<!--<p id="links_menu"><a href="/master.jsp">Master</a>, <a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a></p>-->
<div class="navbar  navbar-fixed-top navbar-default">
    <div class="container">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </button>
            <a class="navbar-brand" href="/master-status"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
        </div>
        <div class="collapse navbar-collapse">
            <ul class="nav navbar-nav">
                <li><a href="/master-status">Home</a></li>
                <li><a href="/tablesDetailed.jsp">Table Details</a></li>
                <li><a href="/logs/">Local logs</a></li>
                <li><a href="/logLevel">Log Level</a></li>
                <li><a href="/dump">Debug dump</a></li>
                <li><a href="/jmx">Metrics Dump</a></li>
                <% if (HBaseConfiguration.isShowConfInServlet()) { %>
                <li><a href="/conf">HBase Configuration</a></li>
                <% } %>
            </ul>
        </div><!--/.nav-collapse -->
    </div>
</div>
<div class="container">
<div class="row inner_header">
    <div class="page-header">
        <h1 id="page_title">HBase Table Query Tool</h1>
    </div>
</div>
<h2>Table name: <%= tableName %></h2>
<table style="border-style: none" width="100%">
<form method="get">
  <input type="hidden" name="name" value="<%= tableName %>">
  <tr>
    <td style="border-style: none">Row Key : <input type="text" name="key" size="128" value="<%=rowKey%>"></td>
    <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Query">
    </td>
  </tr>
  
  <%
    if(displayRowkeyType){ 
  %>
  <tr>
    <td style="border-style: none">
      <input type="radio" name="rowkey_type" value="rowkey" <%=reversedUrlChecked%>>Reversed URL &nbsp; 
      <input type="radio" name="rowkey_type" value="url" <%=urlChecked%>>URL &nbsp; 
    </td>
    <td style="border-style: none" width="5%">&nbsp;</td>
  </tr>
  <%
    }
  %>
  <tr>
    <td style="border-style: none">Max Versions : <input type="text" name="max_versions" value="<%=maxVersions%>"> &nbsp;</td>
    <td style="border-style: none" width="5%">&nbsp;</td>
  </tr>
  <tr>
    <td style="border-style: none">Families :
  <%
    HTableDescriptor htd = admin.getTableDescriptor(tableName.getBytes());
	HColumnDescriptor[] hcds = htd.getColumnFamilies();
	for(HColumnDescriptor hcd:hcds){
  %>
    <input type="checkbox" name="families" value="<%=hcd.getNameAsString()%>"
    <%
      if(familyList.contains(hcd.getNameAsString())){
    %>  
     checked
    <%
      }
    %>  
    > <%=hcd.getNameAsString()%> &nbsp; 
  <%
    }
  %>  
    </td>
    <td style="border-style: none" width="5%">&nbsp;</td>
  </tr>
</form>
</table>
<br>
<table class="table table-striped" width="100%" style="table-layout:fixed;word-break:break-all;word-wrap:break-all;" border="2">
    <tr>
      <th>Family Name</th>
      <th>Qualifier Name</th>
      <th>Timestamp</th>
      <th width="40%">Value</th>
    </tr>  
    <%
    if(!result.isEmpty())
    {
      SimpleDateFormat sdf = new SimpleDateFormat("z E yyyy-MM-dd HH:mm:ss,SSS");
      NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> famliyMap = result.getMap();
      for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : famliyMap.entrySet()) {
        boolean firstFamily = true;
        byte[] family = familyEntry.getKey();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap = familyEntry.getValue();
        long kvCountInFamily = 0;
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry : qualifierMap.entrySet()) {
          byte[] qualifier = qualifierEntry.getKey();
          NavigableMap<Long, byte[]> versionMap = qualifierEntry.getValue();
          kvCountInFamily += versionMap.size();             
        }
        
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry : qualifierMap.entrySet()) {
          boolean firstQualifier = true;
          byte[] qualifier = qualifierEntry.getKey();
          NavigableMap<Long, byte[]> versionMap = qualifierEntry.getValue();
          for (Map.Entry<Long, byte[]> versionEntry : versionMap.entrySet()){ 
            long timestamp = versionEntry.getKey();
            byte[] value = versionEntry.getValue();
    %>
    <tr>
          <%
            if(firstFamily){
          %>
      <td rowspan="<%=Long.toString(kvCountInFamily)%>"><%= Bytes.toString(family) %></td>
          <%         
              firstFamily = false;    
            }
            if(firstQualifier){
          %>
      <td rowspan="<%=Integer.toString(versionMap.size())%>"><%= Bytes.toString(qualifier) %></td>  
          <%
              firstQualifier = false;  
            }
          %>
      <td><%= Long.toString(timestamp) %><br><%= sdf.format(new Date(timestamp)) %></td>       
      <td width="50%"><%= StringEscapeUtils.escapeHtml(Bytes.toString(value)) %></td>
    </tr>
    <%
          }
        }
      }      
      HConnectionManager.deleteConnection(admin.getConfiguration()); 
    }
    %>
</table>
</div>
</body>
</html>
