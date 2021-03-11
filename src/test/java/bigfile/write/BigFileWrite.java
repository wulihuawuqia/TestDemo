package bigfile.write;

import com.alibaba.fastjson.JSON;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.oracle.jrockit.jfr.Producer;
import domain.Upc;
import lombok.Data;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: TestSe
 * @description: 大文件写入测试
 * @author: wuqia
 * @date: 2021-03-08 19:20
 **/
public class BigFileWrite {

    public static final AtomicInteger COUNT = new AtomicInteger(10000000);

    public static LinkedBlockingQueue<String> concurrentLinkedQueue = new LinkedBlockingQueue<>();

    public byte[] lock = new byte[1];

    public static final int RPODUCE_NUM = 4;

    public static final String FILE_URL = "D:\\logs\\test.txt";

    ThreadPoolExecutor  threadPoolExecutor = new ThreadPoolExecutor(8, 8,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    @Test
    public void writeTest() {
        try (
                FileWriter writer = new FileWriter(FILE_URL)
        ){
            for (int i = 0; i < COUNT.get(); i++) {
                writer.append(JSON.toJSONString(Upc.getInstance()));
                writer.append("\n");
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }

    @Test
    public void syncWriteTest() throws InterruptedException, IOException {
        FileWriter writer = null;

        writer = new FileWriter(FILE_URL);

        for (int i = 0; i < RPODUCE_NUM; i++) {
            threadPoolExecutor.execute(() -> {
                while (COUNT.getAndDecrement() > 0) {
                    concurrentLinkedQueue.add(JSON.toJSONString(Upc.getInstance()));
                }
            });
        }
        int threadNum = 2;
        CountDownLatch downLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            FileWriter finalWriter = writer;
            threadPoolExecutor.execute(() -> {
                while (COUNT.get() > 0 || concurrentLinkedQueue.size() > 0) {
                    try {
                        String result = concurrentLinkedQueue.poll();
                        if (null != result) {
                            finalWriter.append(result);
                            finalWriter.append("\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                downLatch.countDown();
            });
        }
        downLatch.await();
        writer.flush();
        writer.close();
    }

    @Test
    public void syncWriteBufferTest() throws InterruptedException, IOException {
        BufferedWriter writer = null;

        writer = new BufferedWriter(new FileWriter(FILE_URL));

        for (int i = 0; i < RPODUCE_NUM; i++) {
            threadPoolExecutor.execute(() -> {
                while (COUNT.getAndDecrement() > 0) {
                    concurrentLinkedQueue.add(JSON.toJSONString(Upc.getInstance()));
                }
            });
        }
        int threadNum = 2;
        CountDownLatch downLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            BufferedWriter finalWriter = writer;
            threadPoolExecutor.execute(() -> {
                while (COUNT.get() > 0 || concurrentLinkedQueue.size() > 0) {
                    try {
                        String result = concurrentLinkedQueue.poll();
                        if (null != result) {
                            finalWriter.append(result);
                            finalWriter.append("\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                downLatch.countDown();
            });
        }
        downLatch.await();
        writer.flush();
        writer.close();
    }

    @Test
    public void syncDisruptorTest() {
        FileWriter writer = null;
        try {
            writer = new FileWriter(FILE_URL);
        } catch (IOException e) {
            e.printStackTrace();
        }
        CountDownLatch downLatch = new CountDownLatch(COUNT.get());
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 2048;

        // Construct the Disruptor
        Disruptor<StringEvent> disruptor = new Disruptor<>(StringEvent::new,
                bufferSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new YieldingWaitStrategy());

        StringEventHandler stringEventHandler = new StringEventHandler(downLatch, writer);

        // Connect the handler
        disruptor.handleEventsWith(stringEventHandler);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<StringEvent> ringBuffer = disruptor.getRingBuffer();

        //模拟多个生产者
        for (int i = 0; i < RPODUCE_NUM; i++) {
            final int a = i;
            threadPoolExecutor.execute(() ->
                    //将ringBuffer 和 生产者ID传入
                    new Producer(ringBuffer, a).process()
            );
        }
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void syncDisruptorWriteBufferTest()  throws InterruptedException, IOException {
        CountDownLatch downLatch = new CountDownLatch(COUNT.get());
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 2048;

        // Construct the Disruptor
        Disruptor<StringEvent> disruptor = new Disruptor<>(StringEvent::new,
                bufferSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new YieldingWaitStrategy());
        BufferedWriter writer = null;

        writer = new BufferedWriter(new FileWriter(FILE_URL));

        StringWriteBufferEventHandler stringEventHandler = new StringWriteBufferEventHandler(downLatch, writer);

        // Connect the handler
        disruptor.handleEventsWith(stringEventHandler);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<StringEvent> ringBuffer = disruptor.getRingBuffer();

        //模拟多个生产者
        for (int i = 0; i < RPODUCE_NUM; i++) {
            final int a = i;
            threadPoolExecutor.execute(() ->
                    //将ringBuffer 和 生产者ID传入
                    new Producer(ringBuffer, a).process()
            );
        }
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        writer.flush();
        writer.close();
    }

    @Data
    static class StringEventHandler implements EventHandler<StringEvent> {

        private StringEventHandler(){}

        public StringEventHandler(CountDownLatch countDownLatch, FileWriter writer) {
            this.writer = writer;
            this.countDownLatch = countDownLatch;
        }

        private CountDownLatch countDownLatch;
        private FileWriter writer;

        public void onEvent(StringEvent event, long sequence, boolean endOfBatch) {
            try {
                writer.append(event.getInfo());
                writer.append("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            countDownLatch.countDown();
        }
    }

    @Test
    public void syncDisruptorByteBufferTest()  throws InterruptedException, IOException {
        CountDownLatch downLatch = new CountDownLatch(COUNT.get());
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 2048;

        // Construct the Disruptor
        Disruptor<StringEvent> disruptor = new Disruptor<>(StringEvent::new,
                bufferSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new YieldingWaitStrategy());

        RandomAccessFile acf = new RandomAccessFile(new File(FILE_URL), "rw");

        StringByteBufferEventHandler stringEventHandler = new StringByteBufferEventHandler(downLatch,
                acf.getChannel());

        // Connect the handler
        disruptor.handleEventsWith(stringEventHandler);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<StringEvent> ringBuffer = disruptor.getRingBuffer();

        //模拟多个生产者
        for (int i = 0; i < RPODUCE_NUM; i++) {
            final int a = i;
            threadPoolExecutor.execute(() ->
                    //将ringBuffer 和 生产者ID传入
                    new Producer(ringBuffer, a).process()
            );
        }
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void syncDisruptorMappedByteBufferTest()  throws InterruptedException, IOException {
        CountDownLatch downLatch = new CountDownLatch(COUNT.get());
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 2048;

        // Construct the Disruptor
        Disruptor<StringEvent> disruptor = new Disruptor<>(StringEvent::new,
                bufferSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new YieldingWaitStrategy());

        RandomAccessFile acf = new RandomAccessFile(new File(FILE_URL), "rw");

        StringMappedByteBufferEventHandler stringEventHandler = new StringMappedByteBufferEventHandler(downLatch,
                acf.getChannel());

        // Connect the handler
        disruptor.handleEventsWith(stringEventHandler);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<StringEvent> ringBuffer = disruptor.getRingBuffer();

        //模拟多个生产者
        for (int i = 0; i < RPODUCE_NUM; i++) {
            final int a = i;
            threadPoolExecutor.execute(() ->
                    //将ringBuffer 和 生产者ID传入
                    new Producer(ringBuffer, a).process()
            );
        }
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Data
    static class StringByteBufferEventHandler implements EventHandler<StringEvent> {

        private StringByteBufferEventHandler(){}

        public StringByteBufferEventHandler(CountDownLatch countDownLatch, FileChannel fileChannel) {
            this.fileChannel = fileChannel;
            this.countDownLatch = countDownLatch;
        }

        private CountDownLatch countDownLatch;
        private FileChannel fileChannel;

        private long offset = 0;

        public void onEvent(StringEvent event, long sequence, boolean endOfBatch) {

            append(event.getInfo());
            append("\n");

            countDownLatch.countDown();
        }

        public void append(String info) {
            byte[] bs = info.getBytes(StandardCharsets.UTF_8);
            int len = bs.length;
            ByteBuffer bbuf = ByteBuffer.allocate(len);
            bbuf.put(bs);
            bbuf.flip();
            try {
                fileChannel.position(offset);
                fileChannel.write(bbuf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            offset = offset + len;
        }
    }

    @Data
    static class StringMappedByteBufferEventHandler implements EventHandler<StringEvent> {

        private StringMappedByteBufferEventHandler(){}

        public StringMappedByteBufferEventHandler(CountDownLatch countDownLatch, FileChannel fileChannel) {
            this.fileChannel = fileChannel;
            this.countDownLatch = countDownLatch;
        }

        private CountDownLatch countDownLatch;
        private FileChannel fileChannel;

        private long offset = 0;

        public void onEvent(StringEvent event, long sequence, boolean endOfBatch) {

            append(event.getInfo());
            append("\n");

            countDownLatch.countDown();
        }

        public void append(String info) {
            byte[] bs = info.getBytes(StandardCharsets.UTF_8);
            int len = bs.length;
            MappedByteBuffer mbuf = null;
            try {
                mbuf = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, len);
            } catch (IOException e) {
                e.printStackTrace();
            }
            mbuf.put(bs);
            offset = offset + len;
        }
    }

    @Data
    static class StringWriteBufferEventHandler implements EventHandler<StringEvent> {

        private StringWriteBufferEventHandler(){}

        public StringWriteBufferEventHandler(CountDownLatch countDownLatch, BufferedWriter writer) {
            this.writer = writer;
            this.countDownLatch = countDownLatch;
        }

        private CountDownLatch countDownLatch;
        private BufferedWriter writer;

        public void onEvent(StringEvent event, long sequence, boolean endOfBatch) {
            try {
                writer.append(event.getInfo());
                writer.append("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            countDownLatch.countDown();
        }
    }

    @Data
    static class StringEvent {
        private String info;
    }

    static class Producer {
        private RingBuffer<StringEvent> ringBuffer;
        private Integer produceId;

        public static void translate(StringEvent event, long sequence, String buffer) {
            event.setInfo(buffer);

        }

        public Producer(RingBuffer<StringEvent> ringBuffer, Integer produceId) {
            this.ringBuffer = ringBuffer;
            this.produceId = produceId;
        }

        public void process(){
            for (int l = 0; l < COUNT.get() / RPODUCE_NUM; l++) {
                ringBuffer.publishEvent(Producer::translate, JSON.toJSONString(Upc.getInstance()));
            }
        }
    }

}
