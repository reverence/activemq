package org.apache.activemq;

import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.util.ByteSequence;

import java.io.File;

/**
 * @ClassName Main
 * @Description TODO
 * @Author chengzhang088
 * @Date 2019/5/7 19:29
 * @Version 1.0
 **/
public class Main {

    public static void main(String[] args) throws Exception{

        /*File dir = new File("D:\\activemq\\test");
        dir.mkdirs();

        Journal dataManager = new Journal();
        dataManager.setMaxFileLength(1*1024);
        dataManager.setDirectory(dir);
        dataManager.start();

        ByteSequence write = new ByteSequence("Hello world1".getBytes());
        Location location = dataManager.write(write, true);

        write = new ByteSequence("Hello world2".getBytes());
        Location location2 = dataManager.write(write, true);

        ByteSequence read = dataManager.read(location);
        System.out.println(new String(read.data));

        read = dataManager.read(location2);
        System.out.println(new String(read.data));*/

        File dir = new File("D:\\activemq\\test");
        dir.mkdirs();
        PageFile pf = new PageFile(dir, "myfile");
        pf.load();
//写入
        Transaction tx = pf.tx();
        Page<String> page = tx.allocate();
        page.set("Hello world");
        tx.store(page, StringMarshaller.INSTANCE, true);
        tx.commit();
        pf.unload();
        pf.load();
//读取
        tx = pf.tx();
        tx.load(page.getPageId(), StringMarshaller.INSTANCE);
        String hello = page.get();
    }
}
