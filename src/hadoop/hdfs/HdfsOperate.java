package hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HdfsOperate {
	
	private static String HDFSUri="hdfs://localhost:9000/";
	
	/**
	 * 获取文件系统
	 * 
	 * @return FileSystem
	 */
	public static FileSystem getFileSystem() {
	    //读取配置文件
	    Configuration conf = new Configuration();
	    // 文件系统
	    FileSystem fs = null;
	    
	    String hdfsUri = HDFSUri;
	    if(StringUtils.isBlank(hdfsUri)){
	        // 返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
	        try {
	            fs = FileSystem.get(conf);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }else{
	        // 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
	        try {
	            URI uri = new URI(hdfsUri.trim());
	            fs = FileSystem.get(uri,conf);
	        } catch (URISyntaxException | IOException e) {
	        	e.printStackTrace();
	        }
	    }
	        
	    return fs;
	}
	
	/**
	 * 创建文件目录
	 * 
	 * @param path
	 */
	public static void mkdir(String path) {
	    try {
	        // 获取文件系统
	        FileSystem fs = getFileSystem();
	        
	        String hdfsUri = HDFSUri;
	        if(StringUtils.isNotBlank(hdfsUri)){
	            path = hdfsUri + path;
	        }
	        
	        // 创建目录
	        fs.mkdirs(new Path(path));
	        System.out.println(String.format("mkdir '%s' suc!", path));
	        
	        //释放资源
	        fs.close();
	    } catch (IllegalArgumentException | IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	/**
	 * 删除文件或者文件目录
	 * 
	 * @param path
	 */
	public static void rmdir(String path) {
	    try {
	        // 返回FileSystem对象
	        FileSystem fs = getFileSystem();
	        
	        String hdfsUri = HDFSUri;
	        if(StringUtils.isNotBlank(hdfsUri)){
	            path = hdfsUri + path;
	        }
	        
	        // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
	        fs.delete(new Path(path),true);
	        System.out.println(String.format("rmdir '%s' suc!", path));
	        
	        // 释放资源
	        fs.close();
	    } catch (IllegalArgumentException | IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	/**
	 * 根据filter获取目录下的文件
	 * 
	 * @param path
	 * @param pathFilter
	 * @return String[]
	 */
	public static String[] listFile(String path,PathFilter pathFilter) {
	    String[] files = new String[0];
	    
	    try {
	        // 返回FileSystem对象
	        FileSystem fs = getFileSystem();
	        
	        String hdfsUri = HDFSUri;
	        if(StringUtils.isNotBlank(hdfsUri)){
	            path = hdfsUri + path;
	        }
	        
	        FileStatus[] status;
	        if(pathFilter != null){
	            // 根据filter列出目录内容
	            status = fs.listStatus(new Path(path),pathFilter);
	        }else{
	            // 列出目录内容
	            status = fs.listStatus(new Path(path));
	        }
	        
	        // 获取目录下的所有文件路径
	        Path[] listedPaths = FileUtil.stat2Paths(status);
	        // 转换String[]
	        if (listedPaths != null && listedPaths.length > 0){
	            files = new String[listedPaths.length];
	            for (int i = 0; i < files.length; i++){
	                files[i] = listedPaths[i].toString();
	            }
	        }
	        // 释放资源
	        fs.close();
	    } catch (IllegalArgumentException | IOException e) {
	    	e.printStackTrace();
	    }
	    
	    return files;
	}
	
	/**
	 * 文件上传至 HDFS
	 * 
	 * @param delSrc
	 * @param overwrite
	 * @param srcFile
	 * @param destPath
	 */
	public static void uploadToHDFS(boolean delSrc, boolean overwrite,String srcFile,String destPath) {
	    // 源文件路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/weibo.txt
	    Path srcPath = new Path(srcFile);
	    
	    // 目的路径
	    String hdfsUri = HDFSUri;
	    if(StringUtils.isNotBlank(hdfsUri)){
	        destPath = hdfsUri + destPath;
	    }
	    Path dstPath = new Path(destPath);
	    
	    // 实现文件上传
	    try {
	        // 获取FileSystem对象
	        FileSystem fs = getFileSystem();
	        fs.copyFromLocalFile(delSrc, overwrite, srcPath, dstPath);
	        System.out.println(String.format("upload '%s' to '%s' suc!", srcFile, destPath));
	        //释放资源
	        fs.close();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	/**
	 * 从 HDFS 下载文件
	 * 
	 * @param srcFilePath
	 * @param destPath
	 */
	public static void downloadFromHDFS(String srcFilePath,String destPath) {
	    // 源文件路径
	    String hdfsUri = HDFSUri;
	    if(StringUtils.isNotBlank(hdfsUri)){
	    	srcFilePath = hdfsUri + srcFilePath;
	    }
	    Path srcPath = new Path(srcFilePath);
	    
	    // 目的路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/
	    Path dstPath = new Path(destPath);
	    
	    try {
	        // 获取FileSystem对象
	        FileSystem fs = getFileSystem();
	        // 下载hdfs上的文件
	        fs.copyToLocalFile(srcPath, dstPath);
	        System.out.println(String.format("download to '%s' from '%s' suc!", destPath, srcFilePath));
	        // 释放资源
	        fs.close();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	/**
	 * 获取 HDFS 集群节点信息
	 * 
	 * @return DatanodeInfo[]
	 */
	public static DatanodeInfo[] getHDFSNodes() {
	    // 获取所有节点
	    DatanodeInfo[] dataNodeStats = new DatanodeInfo[0];
	    
	    try {
	        // 返回FileSystem对象
	        FileSystem fs = getFileSystem();
	        
	        // 获取分布式文件系统
	        DistributedFileSystem hdfs = (DistributedFileSystem)fs;
	        
	        dataNodeStats = hdfs.getDataNodeStats();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	    return dataNodeStats;
	}
	
	/**
	 * 查找某个文件在 HDFS集群的位置
	 * 
	 * @param filePath
	 * @return BlockLocation[]
	 */
	public static BlockLocation[] getFileBlockLocations(String filePath) {
	    // 文件路径
	    String hdfsUri = HDFSUri;
	    if(StringUtils.isNotBlank(hdfsUri)){
	        filePath = hdfsUri + filePath;
	    }
	    Path path = new Path(filePath);
	    
	    // 文件块位置列表
	    BlockLocation[] blkLocations = new BlockLocation[0];
	    try {
	        // 返回FileSystem对象
	        FileSystem fs = getFileSystem();
	        // 获取文件目录 
	        FileStatus filestatus = fs.getFileStatus(path);
	        //获取文件块位置列表
	        blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	    return blkLocations;
	}
	
	/**
	 * 文件重命名
	 * 
	 * @param srcPath
	 * @param dstPath
	 */
	public static boolean rename(String srcPath, String dstPath){
	    boolean flag = false;
	    try    {
	        // 返回FileSystem对象
	        FileSystem fs = getFileSystem();
	        
	        String hdfsUri = HDFSUri;
	        if(StringUtils.isNotBlank(hdfsUri)){
	            srcPath = hdfsUri + srcPath;
	            dstPath = hdfsUri + dstPath;
	        }
	        
	        flag = fs.rename(new Path(srcPath), new Path(dstPath));
	        System.out.println(String.format("'%s' rename to '%s' suc!", srcPath, dstPath));
	    } catch (IOException e) {
	        System.out.println(String.format("'%s' rename to '%s' error.", srcPath, dstPath));
	        e.printStackTrace();
	    }
	    
	    return flag;
	}
	
	/**
	 * 判断目录是否存在
	 * 
	 * @param dirPath
	 * @param create 不存在是否mkdir
	 */
	public static boolean existDir(String dirPath, boolean create){
	    boolean flag = false;
	    
	    if (StringUtils.isEmpty(dirPath)){
	        return flag;
	    }
	    
	    try{
	        Path path = new Path(dirPath);
	        // FileSystem对象
	        FileSystem fs = getFileSystem();
	        
	        if (create){
	            if (!fs.exists(path)){
	                fs.mkdirs(path);
	            }
	        }
	        
	        if (fs.isDirectory(path)){
	            flag = true;
	        }
	    }catch (Exception e){
	    	e.printStackTrace();
	    }
	    
	    return flag;
	}
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, IllegalArgumentException, IllegalAccessException, InvocationTargetException{
//		mkdir("in");
		
//		System.out.println(existDir(HDFSUri+"in", false));
		
//		uploadToHDFS(true, true, "C:\\Users\\chency\\Desktop\\jd-gui.cfg", "in");
		
//		System.out.println(Arrays.toString(listFile("in", null)));
		
//		downloadFromHDFS("in/jd-gui.cfg", "C:\\Users\\chency\\Desktop\\");
		
//		rename("in/jd-gui.cfg", "in/jd-gui222.cfg");
		
////		System.out.println(Arrays.toString(getFileBlockLocations("in/jd-gui222.cfg")));
//		Class<?> cls=Class.forName("org.apache.hadoop.fs.BlockLocation");
//		Field [] fields = cls.getDeclaredFields();
//		Method[] methods=cls.getDeclaredMethods();
//		BlockLocation[] blkLocations=getFileBlockLocations("in/jd-gui222.cfg");
//		for(BlockLocation blockLocation:blkLocations){
//			System.out.println(blockLocation.toString());
//			for(Field f:fields){
//				f.setAccessible(true);
//				String name=f.getName();
//				Object val=f.get(blockLocation);
//				if(val instanceof String[]){
//					System.out.println(String.format("blockLocation.%s=%s", name, Arrays.toString((String[]) val)));
//				}else{
//					System.out.println(String.format("blockLocation.%s=%s", name, val));
//				}
//				
//			}
//			for(Method m:methods){
//				m.setAccessible(true);
//				String name=m.getName();
//				if(name.indexOf("get")>=0){
//					Object val=m.invoke(blockLocation);
//					if(val instanceof String[]){
//						System.out.println(String.format("blockLocation.%s=%s", name, Arrays.toString((String[]) val)));
//					}else{
//						System.out.println(String.format("blockLocation.%s=%s", name, val));
//					}
//					
//				}
//			}
//		}
		
////		System.out.println(Arrays.toString(getHDFSNodes()));
//		DatanodeInfo[] dataNodeStats=getHDFSNodes();
//		Class<?> cls=Class.forName("org.apache.hadoop.hdfs.protocol.DatanodeInfo");
//		Field [] fields = cls.getDeclaredFields();
//		Method[] methods=cls.getDeclaredMethods();
//		for(DatanodeInfo datanodeInfo:dataNodeStats){
//			System.out.println(datanodeInfo.toString());
//			for(Field f:fields){
//				f.setAccessible(true);
//				String name=f.getName();
//				System.out.println(String.format("datanodeInfo.%s=%s", name, f.get(datanodeInfo)));
//			}
//			for(Method m:methods){
//				m.setAccessible(true);
//				String name=m.getName();
//				if(name.indexOf("get")>=0){
//					System.out.println(String.format("datanodeInfo.%s=%s", name, m.invoke(datanodeInfo)));
//				}
//			}
//		}
		
		rmdir("in");
		
	}

}
