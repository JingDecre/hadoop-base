package com.decre.hadoop.hadoopbase;

import com.decre.hadoop.hadoopbase.entity.User;
import com.decre.hadoop.hadoopbase.service.HdfsService;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@WebAppConfiguration
public class HadoopBaseApplicationTests {

    @Autowired
    private HdfsService hdfsService;


    /**
     * 测试创建HDFS目录
     */
    @Test
    public void testMkdir() {
        boolean result1 = hdfsService.mkdir("/testDir");
        System.out.println("创建结果：" + result1);

        boolean result2 = hdfsService.mkdir("/testDir/subDir");
        System.out.println("创建结果：" + result2);
    }

    /**
     * 测试上传文件
     */
    @Test
    public void testUploadFile() {
        hdfsService.uploadFileToHdfs("D:\\ITape\\test\\hadoop1.txt", "/testDir");
        hdfsService.uploadFileToHdfs(false, true, "D:\\ITape\\test\\hadoop2.txt", "/testDir");
        hdfsService.uploadFileToHdfs("D:\\ITape\\test\\hadoop3.txt", "/testDir/subDir");

    }

    /**
     * 测试列出某个目录下面的文件
     */
    @Test
    public void testListFiles() {

        List<Map<String, Object>> result = hdfsService.listFiles("/testDir", null);
        result.forEach(map -> {
            map.forEach((k, v) -> {
                System.out.println(k + "--" + v);
            });
            System.out.println();
        });
    }

    /**
     * 测试从hdfs上下载文件
     */
    @Test
    public void testDownloadFile() {
        hdfsService.downloadFileFromHdfs("/testDir/hadoop1.txt", "D:/ITape/test/download1.txt");
    }

    /**
     * 测试打开hdfs上的文件，并转换成字符串
     *
     * @throws IOException
     */
    @Test
    public void testOpen() throws IOException {
        FSDataInputStream inputStream = hdfsService.open("/testDir/hadoop1.txt");

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        reader.close();
    }

    /**
     * 测试打开HDFS上面的文件，并转化为Java对象
     *
     * @throws IOException
     */
    @Test
    public void testOpenWithObject() throws IOException {
        User user = hdfsService.openWithObject("/testDir/hadoop2.txt", User.class);
        System.out.println(user);
    }

    /**
     * 测试重命名
     */
    @Test
    public void testRename() {
        hdfsService.rename("/testDir/hadoop2.txt", "/testDir/user.txt");

        //再次遍历
        testListFiles();
    }


    /**
     * 测试删除文件
     */
    @Test
    public void testDelete() {
        hdfsService.delete("/testDir/hadoop1.txt");

        //再次遍历
        testListFiles();
    }

    /**
     * 测试获取某个文件在HDFS集群的位置
     *
     * @throws IOException
     */
    @Test
    public void testGetFileBlockLocations() throws IOException {
        BlockLocation[] locations = hdfsService.getFileBlockLocations("/testDir/user.txt");

        if (locations != null && locations.length > 0) {
            for (BlockLocation location : locations) {
                System.out.println(location.getHosts()[0]);
            }
        }
    }
}
