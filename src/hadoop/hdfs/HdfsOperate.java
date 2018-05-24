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
	 * ��ȡ�ļ�ϵͳ
	 * 
	 * @return FileSystem
	 */
	public static FileSystem getFileSystem() {
	    //��ȡ�����ļ�
	    Configuration conf = new Configuration();
	    // �ļ�ϵͳ
	    FileSystem fs = null;
	    
	    String hdfsUri = HDFSUri;
	    if(StringUtils.isBlank(hdfsUri)){
	        // ����Ĭ���ļ�ϵͳ  ����� Hadoop��Ⱥ�����У�ʹ�ô��ַ�����ֱ�ӻ�ȡĬ���ļ�ϵͳ
	        try {
	            fs = FileSystem.get(conf);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }else{
	        // ����ָ�����ļ�ϵͳ,����ڱ��ز��ԣ���Ҫʹ�ô��ַ�����ȡ�ļ�ϵͳ
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
	 * �����ļ�Ŀ¼
	 * 
	 * @param path
	 */
	public static void mkdir(String path) {
	    try {
	        // ��ȡ�ļ�ϵͳ
	        FileSystem fs = getFileSystem();
	        
	        String hdfsUri = HDFSUri;
	        if(StringUtils.isNotBlank(hdfsUri)){
	            path = hdfsUri + path;
	        }
	        
	        // ����Ŀ¼
	        fs.mkdirs(new Path(path));
	        System.out.println(String.format("mkdir '%s' suc!", path));
	        
	        //�ͷ���Դ
	        fs.close();
	    } catch (IllegalArgumentException | IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	/**
	 * ɾ���ļ������ļ�Ŀ¼
	 * 
	 * @param path
	 */
	public static void rmdir(String path) {
	    try {
	        // ����FileSystem����
	        FileSystem fs = getFileSystem();
	        
	        String hdfsUri = HDFSUri;
	        if(StringUtils.isNotBlank(hdfsUri)){
	            path = hdfsUri + path;
	        }
	        
	        // ɾ���ļ������ļ�Ŀ¼  delete(Path f) �˷����Ѿ�����
	        fs.delete(new Path(path),true);
	        System.out.println(String.format("rmdir '%s' suc!", path));
	        
	        // �ͷ���Դ
	        fs.close();
	    } catch (IllegalArgumentException | IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	/**
	 * ����filter��ȡĿ¼�µ��ļ�
	 * 
	 * @param path
	 * @param pathFilter
	 * @return String[]
	 */
	public static String[] listFile(String path,PathFilter pathFilter) {
	    String[] files = new String[0];
	    
	    try {
	        // ����FileSystem����
	        FileSystem fs = getFileSystem();
	        
	        String hdfsUri = HDFSUri;
	        if(StringUtils.isNotBlank(hdfsUri)){
	            path = hdfsUri + path;
	        }
	        
	        FileStatus[] status;
	        if(pathFilter != null){
	            // ����filter�г�Ŀ¼����
	            status = fs.listStatus(new Path(path),pathFilter);
	        }else{
	            // �г�Ŀ¼����
	            status = fs.listStatus(new Path(path));
	        }
	        
	        // ��ȡĿ¼�µ������ļ�·��
	        Path[] listedPaths = FileUtil.stat2Paths(status);
	        // ת��String[]
	        if (listedPaths != null && listedPaths.length > 0){
	            files = new String[listedPaths.length];
	            for (int i = 0; i < files.length; i++){
	                files[i] = listedPaths[i].toString();
	            }
	        }
	        // �ͷ���Դ
	        fs.close();
	    } catch (IllegalArgumentException | IOException e) {
	    	e.printStackTrace();
	    }
	    
	    return files;
	}
	
	/**
	 * �ļ��ϴ��� HDFS
	 * 
	 * @param delSrc
	 * @param overwrite
	 * @param srcFile
	 * @param destPath
	 */
	public static void uploadToHDFS(boolean delSrc, boolean overwrite,String srcFile,String destPath) {
	    // Դ�ļ�·����Linux�µ�·��������� windows �²��ԣ���Ҫ��дΪWindows�µ�·��������D://hadoop/djt/weibo.txt
	    Path srcPath = new Path(srcFile);
	    
	    // Ŀ��·��
	    String hdfsUri = HDFSUri;
	    if(StringUtils.isNotBlank(hdfsUri)){
	        destPath = hdfsUri + destPath;
	    }
	    Path dstPath = new Path(destPath);
	    
	    // ʵ���ļ��ϴ�
	    try {
	        // ��ȡFileSystem����
	        FileSystem fs = getFileSystem();
	        fs.copyFromLocalFile(delSrc, overwrite, srcPath, dstPath);
	        System.out.println(String.format("upload '%s' to '%s' suc!", srcFile, destPath));
	        //�ͷ���Դ
	        fs.close();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	/**
	 * �� HDFS �����ļ�
	 * 
	 * @param srcFilePath
	 * @param destPath
	 */
	public static void downloadFromHDFS(String srcFilePath,String destPath) {
	    // Դ�ļ�·��
	    String hdfsUri = HDFSUri;
	    if(StringUtils.isNotBlank(hdfsUri)){
	    	srcFilePath = hdfsUri + srcFilePath;
	    }
	    Path srcPath = new Path(srcFilePath);
	    
	    // Ŀ��·����Linux�µ�·��������� windows �²��ԣ���Ҫ��дΪWindows�µ�·��������D://hadoop/djt/
	    Path dstPath = new Path(destPath);
	    
	    try {
	        // ��ȡFileSystem����
	        FileSystem fs = getFileSystem();
	        // ����hdfs�ϵ��ļ�
	        fs.copyToLocalFile(srcPath, dstPath);
	        System.out.println(String.format("download to '%s' from '%s' suc!", destPath, srcFilePath));
	        // �ͷ���Դ
	        fs.close();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	/**
	 * ��ȡ HDFS ��Ⱥ�ڵ���Ϣ
	 * 
	 * @return DatanodeInfo[]
	 */
	public static DatanodeInfo[] getHDFSNodes() {
	    // ��ȡ���нڵ�
	    DatanodeInfo[] dataNodeStats = new DatanodeInfo[0];
	    
	    try {
	        // ����FileSystem����
	        FileSystem fs = getFileSystem();
	        
	        // ��ȡ�ֲ�ʽ�ļ�ϵͳ
	        DistributedFileSystem hdfs = (DistributedFileSystem)fs;
	        
	        dataNodeStats = hdfs.getDataNodeStats();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	    return dataNodeStats;
	}
	
	/**
	 * ����ĳ���ļ��� HDFS��Ⱥ��λ��
	 * 
	 * @param filePath
	 * @return BlockLocation[]
	 */
	public static BlockLocation[] getFileBlockLocations(String filePath) {
	    // �ļ�·��
	    String hdfsUri = HDFSUri;
	    if(StringUtils.isNotBlank(hdfsUri)){
	        filePath = hdfsUri + filePath;
	    }
	    Path path = new Path(filePath);
	    
	    // �ļ���λ���б�
	    BlockLocation[] blkLocations = new BlockLocation[0];
	    try {
	        // ����FileSystem����
	        FileSystem fs = getFileSystem();
	        // ��ȡ�ļ�Ŀ¼ 
	        FileStatus filestatus = fs.getFileStatus(path);
	        //��ȡ�ļ���λ���б�
	        blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	    return blkLocations;
	}
	
	/**
	 * �ļ�������
	 * 
	 * @param srcPath
	 * @param dstPath
	 */
	public static boolean rename(String srcPath, String dstPath){
	    boolean flag = false;
	    try    {
	        // ����FileSystem����
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
	 * �ж�Ŀ¼�Ƿ����
	 * 
	 * @param dirPath
	 * @param create �������Ƿ�mkdir
	 */
	public static boolean existDir(String dirPath, boolean create){
	    boolean flag = false;
	    
	    if (StringUtils.isEmpty(dirPath)){
	        return flag;
	    }
	    
	    try{
	        Path path = new Path(dirPath);
	        // FileSystem����
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
