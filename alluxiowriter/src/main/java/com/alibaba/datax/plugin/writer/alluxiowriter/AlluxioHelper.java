package com.alibaba.datax.plugin.writer.alluxiowriter;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public  class AlluxioHelper {
    public static final Logger LOG = LoggerFactory.getLogger(AlluxioWriter.Job.class);
    public InstancedConfiguration conf = InstancedConfiguration.defaults();
    public FileSystem fileSystem;
    private static final DateFormat dateParse = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public void getFileSystem(String masterHostName, Configuration taskConfig){
        conf.set(PropertyKey.MASTER_HOSTNAME,masterHostName);
        fileSystem = FileSystem.Factory.create(conf);
    }

    /**
     *获取指定目录先的文件列表
     * @param dir
     * @return
     * 拿到的是文件全路径，
     * eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
     */
    public String[] alluxioDirList(String dir){
        LOG.info("list file from path:{}",dir);
        AlluxioURI path = new AlluxioURI(dir);
        String[] files = null;
        try {
            List<URIStatus> status = fileSystem.listStatus(path);
            if(status == null) {
                status = Lists.newArrayList();
            }
            files = new String[status.size()];

            LOG.info("file count from path:{} is:{}",dir,status.size());
            for(int i=0;i<status.size();i++){
                files[i] = status.get(i).getPath();
            }
        } catch (Exception e) {
            String message = String.format("获取目录[%s]文件列表时发生网络IO异常,请检查您的网络是否正常！", dir);
            LOG.error(message);
            throw DataXException.asDataXException(AlluxioWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    /**
     * 获取以fileName__ 开头的文件列表
     * @param dir
     * @param fileName
     * @return
     */
    public AlluxioURI[] alluxioDirList(String dir, String fileName){
        AlluxioURI path = new AlluxioURI(dir);
        AlluxioURI[] files = null;
        String filterFileName = fileName + "__*";
        try {
//            PathFilter pathFilter = new GlobFilter(filterFileName);
            ListStatusPOptions listStatusPOptions = ListStatusPOptions.newBuilder().build();
            List<URIStatus> status = fileSystem.listStatus(path);
            files = new AlluxioURI[status.size()];
            for(int i=0;i<status.size();i++){
                files[i] = new AlluxioURI(status.get(i).getPath());
            }
        } catch (Exception e) {
            String message = String.format("获取目录[%s]下文件名以[%s]开头的文件列表时发生网络IO异常,请检查您的网络是否正常！",
                    dir,fileName);
            LOG.error(message);
            throw DataXException.asDataXException(AlluxioWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    public boolean isPathExists(String filePath) {
        AlluxioURI path = new AlluxioURI(filePath);
        boolean exist = false;
        try {
            exist = fileSystem.exists(path);
            if(!exist) {
                fileSystem.createDirectory(path);
            }
        } catch (Exception e) {
            String message = String.format("判断文件路径[%s]是否存在时发生网络IO异常,请检查您的网络是否正常！",
                    "message:filePath =" + filePath);
            LOG.error(message);
            throw DataXException.asDataXException(AlluxioWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return true;
    }

//    public boolean isPathDir(String filePath) {
//        AlluxioURI path = new AlluxioURI(filePath);
//        boolean isDir = false;
//        try {
////            path.
//        } catch (Exception e) {
//            String message = String.format("判断路径[%s]是否是目录时发生网络IO异常,请检查您的网络是否正常！", filePath);
//            LOG.error(message);
//            throw DataXException.asDataXException(AlluxioWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        }
//        return isDir;
//    }

    public void deleteFiles(AlluxioURI[] paths){
        for(int i=0;i<paths.length;i++){
            LOG.info(String.format("delete file [%s].", paths[i].toString()));
            try {
                fileSystem.delete(paths[i], DeletePOptions.newBuilder().build());
            } catch (Exception e) {
                String message = String.format("删除文件[%s]时发生IO异常,请检查您的网络是否正常！",
                        paths[i].toString());
                LOG.error(message);
                throw DataXException.asDataXException(AlluxioWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }
        }
    }

//    public void deleteDir(AlluxioURI path){
//        LOG.info(String.format("start delete tmp dir [%s] .",path.toString()));
//        try {
//            if(isPathExists(path.toString())) {
//                fileSystem.delete(path, DeletePOptions.newBuilder().setRecursive(true).build());
//            }
//        } catch (Exception e) {
//            String message = String.format("删除临时目录[%s]时发生IO异常,请检查您的网络是否正常！", path.toString());
//            LOG.error(message);
//            throw DataXException.asDataXException(AlluxioWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        }
//        LOG.info(String.format("finish delete tmp dir [%s] .",path.toString()));
//    }

//    public void renameFile(HashSet<String> tmpFiles, HashSet<String> endFiles){
//        AlluxioURI tmpFilesParent = null;
//        if(tmpFiles.size() != endFiles.size()){
//            String message = String.format("临时目录下文件名个数与目标文件名个数不一致!");
//            LOG.error(message);
//            throw DataXException.asDataXException(AlluxioWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
//        }else{
//            try{
//                for (Iterator it1=tmpFiles.iterator(),it2=endFiles.iterator();it1.hasNext()&&it2.hasNext();){
//                    String srcFile = it1.next().toString();
//                    String dstFile = it2.next().toString();
//                    AlluxioURI srcFilePah = new AlluxioURI(srcFile);
//                    AlluxioURI dstFilePah = new AlluxioURI(dstFile);
//                    if(tmpFilesParent == null){
//                        tmpFilesParent = srcFilePah.getParent();
//                    }
//                    LOG.info(String.format("start rename file [%s] to file [%s].", srcFile,dstFile));
//                    boolean renameTag = false;
//                    long fileLen = fileSystem.getStatus(srcFilePah).getLength();
//                    if(fileLen>0){
//                        fileSystem.rename(srcFilePah,dstFilePah);
//                        LOG.info(String.format("finish rename file [%s] to file [%s].", srcFile,dstFile));
//                    }else{
//                        LOG.info(String.format("文件［%s］内容为空,请检查写入是否正常！", srcFile));
//                    }
//                }
//            }catch (Exception e) {
//                String message = String.format("重命名文件时发生异常,请检查您的网络是否正常！");
//                LOG.error(message);
//                throw DataXException.asDataXException(AlluxioWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//            }finally {
//                deleteDir(tmpFilesParent);
//            }
//        }
//    }

    //关闭FileSystem
    public void closeFileSystem(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            String message = String.format("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            LOG.error(message);
            throw DataXException.asDataXException(AlluxioWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }


    //textfile格式文件
    public FileOutStream getOutputStream(String path){
        AlluxioURI storePath = new AlluxioURI(path);
        FileOutStream fileOutStream = null;
        try {
            fileOutStream = fileSystem.createFile(storePath);
        } catch (Exception e) {
            String message = String.format("Create an FSDataOutputStream at the indicated Path[%s] failed: [%s]",
                    "message:path =" + path);
            LOG.error(message);
            throw DataXException.asDataXException(AlluxioWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
        return fileOutStream;
    }

    /**
     * 写textfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void textFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                   TaskPluginCollector taskPluginCollector){
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER);
//        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
//        String compress = config.getString(Key.COMPRESS,null);

        AlluxioURI outputPath = new AlluxioURI(fileName);
        BufferedOutputStream out = null;
        Record record;
        try {
            if(!fileSystem.exists(outputPath)) {
                out = new BufferedOutputStream(fileSystem.createFile(outputPath));
            }
            while ((record = lineReceiver.getFromReader()) != null) {
                List<String> splitedRows = new ArrayList<String>();
                int recordLength = record.getColumnNumber();
                if (0 != recordLength) {
                    Column column;
                    for (int i = 0; i < recordLength; i++) {
                        column = record.getColumn(i);
                        if (null != column.getRawData()) {
                            boolean isDateColumn = column instanceof DateColumn;
                            if (!isDateColumn) {
                                splitedRows.add(column.asString());
                            } else {
                                if (null != dateParse) {
                                    splitedRows.add(dateParse.format(column
                                            .asDate()));
                                } else {
                                    splitedRows.add(column.asString());
                                }
                            }
                        } else {
                            // warn: it's all ok if nullFormat is null
                            splitedRows.add(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.NULL_FORMAT);
                        }
                    }
                }
                splitedRows.add("\n");

                if(CollectionUtils.isNotEmpty(splitedRows)) {
                    out.write((StringUtils.join(splitedRows, fieldDelimiter))
                            .replace(",\n,","\n").replace(",\n","\n")
                            .getBytes());
                }

                splitedRows.clear();
            }
        } catch (Exception e) {
            String message = String.format("写文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            LOG.error(message);
            AlluxioURI path = new AlluxioURI(fileName);
//            deleteDir(path.getParent());
            throw DataXException.asDataXException(AlluxioWriterErrorCode.Write_FILE_IO_ERROR, e);
        } finally {
            if(out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public List<String> getColumnNames(List<Configuration> columns){
        List<String> columnNames = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            columnNames.add(eachColumnConf.getString(Key.NAME));
        }
        return columnNames;
    }
}
