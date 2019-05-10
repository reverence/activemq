package org.apache.activemq.store.kahadb.disk.journal;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertNotNull;

/**
 * @ClassName TestMain
 * @Description TODO
 * @Author chengzhang088
 * @Date 2019/5/10 14:46
 * @Version 1.0
 **/
public class TestMain {

    private static int ONE_MB = 1024*1024;

    private static KahaDBStore store;

    public static void main(String[] args) throws Exception{
        MessageStore messageStore = createStore(8 * ONE_MB);
        addMessages(messageStore, 4);

        long sizeBeforeChange = store.getJournal().getDiskSize();
        System.out.println("Journal size before: " + sizeBeforeChange);

        store.stop();
        messageStore = createStore(6 * ONE_MB);
        verifyMessages(messageStore, 4);

        long sizeAfterChange = store.getJournal().getDiskSize();
        System.out.println("Journal size after: " + sizeAfterChange);
    }

    private static MessageStore createStore(int length) throws Exception {
        File dataDirectory = new File("D:\\activemq\\test");
        store = new KahaDBStore();
        store.setJournalMaxFileLength(length);
        store.setDirectory(dataDirectory);
        store.setForceRecoverIndex(true);
        store.start();
        return store.createQueueMessageStore(new ActiveMQQueue("test"));
    }

    private static void verifyMessages(MessageStore messageStore, int num) throws Exception {
        for (int i=0; i < num; i++) {
            assertNotNull(messageStore.getMessage(new MessageId("1:2:3:" + i)));
        }
    }

    private static void addMessages(MessageStore messageStore, int num) throws Exception {
        String text = getString(ONE_MB);

        for (int i=0; i < num; i++) {
            ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
            textMessage.setMessageId(new MessageId("1:2:3:" + i));
            textMessage.setText(text);
            messageStore.addMessage(new ConnectionContext(), textMessage);
        }
    }

    private static String getString(int size) {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < size; i++) {
            builder.append("a");
        }
        return builder.toString();
    }
}
