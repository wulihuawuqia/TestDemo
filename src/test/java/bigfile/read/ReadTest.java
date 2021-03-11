package bigfile.read;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: TestDemo
 * @description: 文件读取
 * @author: wuqia
 * @date: 2021-03-09 10:34
 **/
@Slf4j
public class ReadTest {

    AtomicInteger count = new AtomicInteger(0);

    @Test
    public void read() {
        try (FileReader fileReader = new FileReader("D:\\logs\\test.txt");
             BufferedReader br = new BufferedReader(fileReader);) {
            String str = br.readLine();
            while (str != null) {
                count.getAndIncrement();
                str = br.readLine();
            }
            log.error("文件总行数,{}", br.lines().count());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
       log.error("count,{}", count.get());
    }


}
