package hadoop.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

public class HdfsDAO {
	
	/**
	 * HDFS������ַ
	 */
    private static final String HDFS = "hdfs://localhost:9000/";
    
    /**
     * ����������
     * @param conf
     */
    public HdfsDAO(Configuration conf) {
        this(HDFS, conf);
    }

    public HdfsDAO(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    private String hdfsPath;
    private Configuration conf;
    
    /**
     * ���Է������
     */
    public static void main(String[] args) throws IOException {
        JobConf conf = config();
        HdfsDAO hdfs = new HdfsDAO(conf);
        
//        hdfs.mkdirs(HDFS+"in");
        
//        hdfs.rmr(HDFS+"user");
        
//        hdfs.copyFile("C:\\Users\\chency\\Desktop\\complaint_20170802.txt", HDFS+"in");

//        hdfs.ls(HDFS+"in");
        
//        hdfs.createFile(HDFS+"in/abc.txt", "abc");

//        hdfs.rename(HDFS+"in/abc.txt", HDFS+"in/abc22.txt");

        hdfs.location(HDFS+"in/abc22.txt");
        hdfs.location(HDFS+"in/complaint_20170802.txt");
        
    }

    public static JobConf config() {
        JobConf conf = new JobConf();
        conf.setJobName("HdfsDAO");
//      conf.addResource("classpath:/hadoop/core-site.xml");
//      conf.addResource("classpath:/hadoop/hdfs-site.xml");
//      conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    /**
     * ����Ŀ¼
     * @param folder
     * @throws IOException
     */
    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }
    /**
     * ɾ���ļ���Ŀ¼
     * @param folder
     * @throws IOException
     */
    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }
    /**
     * �������ļ�
     * @param src
     * @param dst
     * @throws IOException
     */
    public void rename(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }
    /**
     * �����ļ�
     * @param folder
     * @throws IOException
     */
    public void ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }
    /**
     * �����ļ�
     * @param file
     * @param content
     * @throws IOException
     */
    public void createFile(String filePath, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(filePath));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + filePath);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }
    /**
     * �����ļ���HDFS
     * @param local
     * @param remote
     * @throws IOException
     */
    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(false, true, new Path(local), new Path(remote));
        System.out.printf("copy from: %s to %s", local, remote);
        fs.close();
    }
    /**
     * ��HDFS�������ļ���������
     * @param remote
     * @param local
     * @throws IOException
     */
    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }
    /**
     * �鿴�ļ��е�����
     * @param remoteFile
     * @return
     * @throws IOException
     */
    public String cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);

        OutputStream baos = new ByteArrayOutputStream();
        String str = null;
        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, baos, 4096, false);
            str = baos.toString();
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
        System.out.println(str);
        return str;
    }
    /**
     * ���ظ����ļ���λ��
     * Return an array containing hostnames, offset and size of 
     * portions of the given file.
     */
    public void location(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), new Configuration()/*conf*/);
        FileStatus f = fs.getFileStatus(new Path(filePath));
        BlockLocation[] list = fs.getFileBlockLocations(f, 0, f.getLen());
        System.out.println("File Location: " + filePath);
        for (BlockLocation bl : list) {
            String[] hosts = bl.getHosts();
            for (String host : hosts) {
                System.out.println("host:" + host);
            }
        }
        fs.close();
    }
	
}
