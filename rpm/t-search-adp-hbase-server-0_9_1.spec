%define _prefix /home/hadoop/hadoop_hbase
%define VERSION 0.9.1
%define HBASE_DIR_NAME hbase-1.1.2-adp-%{VERSION}

Name:t-search-adp-hbase-server-0_9_1
Version:0.9.1
Release:%{_release}%{?dist}
Summary:Ali Data Platform - HBase
URL:%{_svn_path}
Group:Simba/daogou
License:Commercial
Prefix:%{_prefix}
AutoReq:no

%description
%{_svn_path}
%{_svn_revision}

%build
cd $OLDPWD/..
mvn clean package -DskipTests assembly:single
# recreate tarball in target dir
cd ./hbase-assembly/target
ls -l ./%{HBASE_DIR_NAME}-bin.tar.gz
tar zxf ./%{HBASE_DIR_NAME}-bin.tar.gz
rm -f ./%{HBASE_DIR_NAME}/bin/*.cmd
rm -f ./%{HBASE_DIR_NAME}/conf/*.cmd
rm -f ./%{HBASE_DIR_NAME}/lib/hadoop-*.jar
rm -f ./%{HBASE_DIR_NAME}/lib/hqueue-*.jar
cp ../../admin-support/lib/*.jar ./%{HBASE_DIR_NAME}/lib/
rm -f ./%{HBASE_DIR_NAME}-bin.tar.gz
tar zcf ./%{HBASE_DIR_NAME}-bin.tar.gz ./%{HBASE_DIR_NAME}
ls -l ./%{HBASE_DIR_NAME}-bin.tar.gz
rm -rf ./%{HBASE_DIR_NAME}
# back to hbase home dir
cd ../..

%install
set -x
cd $RPM_BUILD_ROOT
# create directories
mkdir -p ./%{_prefix}

# extract files and directories
cp -rf ../../../hbase-assembly/target/%{HBASE_DIR_NAME}-bin.tar.gz ./%{_prefix}/
cd ./%{_prefix}
tar zxf %{HBASE_DIR_NAME}-bin.tar.gz
rm -f %{HBASE_DIR_NAME}-bin.tar.gz

# change files' attribute
chmod 755 ./%{HBASE_DIR_NAME}/bin/*
chmod 755 ./%{HBASE_DIR_NAME}/conf/*

# set file attribute here
%files
%defattr(-,hadoop,hadoop)
%{_prefix}/%{HBASE_DIR_NAME}

# if the config file exists, don't replace it.
%config(noreplace) %{_prefix}/%{HBASE_DIR_NAME}/conf/hadoop-metrics2-hbase.properties
%config(noreplace) %{_prefix}/%{HBASE_DIR_NAME}/conf/hbase-env.sh
%config(noreplace) %{_prefix}/%{HBASE_DIR_NAME}/conf/hbase-policy.xml
%config(noreplace) %{_prefix}/%{HBASE_DIR_NAME}/conf/hbase-site.xml
%config(noreplace) %{_prefix}/%{HBASE_DIR_NAME}/conf/log4j.properties
%config(noreplace) %{_prefix}/%{HBASE_DIR_NAME}/conf/regionservers

%changelog
