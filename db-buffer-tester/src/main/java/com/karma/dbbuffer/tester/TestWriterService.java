package com.karma.dbbuffer.tester;

import com.karma.dbbuffer.client.BufferedDBWriter;
import com.karma.dbbuffer.schema.OperationType;
import com.karma.dbbuffer.tester.entity.SimpleDefinition;
import com.karma.dbbuffer.tester.entity.SortableDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class TestWriterService {

    private BufferedDBWriter bufferedDBWriter;

    @Autowired(required = false)
    protected void setBufferedDBWriter(BufferedDBWriter bufferedDBWriter) {
        this.bufferedDBWriter = bufferedDBWriter;
    }

    @PostConstruct
    void onInit() throws InterruptedException {

        Thread.sleep(3000L);

        Long[] ownerIds = { 1L, 2L, 3L, 4L, 5L };

        for (long i = 0; i < 1000; i += 4) {
            Thread.sleep(1L);

            Long ownerId = ownerIds[(int) Math.floor(Math.random() * ownerIds.length)];
            Long baseId = i;

            SimpleDefinition simpleTest = new SimpleDefinition();
            simpleTest.setId(baseId);
            simpleTest.setBooleanV(true);
            simpleTest.setByteV((byte) 87);
            simpleTest.setShortV((short) 167);
            simpleTest.setLongV(647);
            simpleTest.setStringO("Hello 997 !");
            simpleTest.setIntegerO(3207);
            simpleTest.setLongO(null);
            bufferedDBWriter.sendRecord(ownerId, simpleTest, OperationType.INSERT);

            simpleTest.setByteV((byte) (8 * 2));
            simpleTest.setShortV((short) (16 * 2));
            simpleTest.setLongV(64 * 2);
            simpleTest.setIntegerO(null);
            bufferedDBWriter.sendRecord(ownerId, simpleTest, OperationType.UPDATE);

            simpleTest.setBooleanV(false);
            simpleTest.setLongO(640L);
            bufferedDBWriter.sendRecord(ownerId, simpleTest, OperationType.UPDATE);

            // 998
            simpleTest = new SimpleDefinition();
            simpleTest.setId(baseId + 1);
            simpleTest.setBooleanV(false);
            simpleTest.setByteV((byte) 88);
            simpleTest.setShortV((short) 168);
            simpleTest.setLongV(648);
            simpleTest.setStringO("Hello 998 !");
            simpleTest.setIntegerO(3208);
            simpleTest.setLongO(null);
            bufferedDBWriter.sendRecord(ownerId, simpleTest, OperationType.INSERT);

            // 999
            simpleTest = new SimpleDefinition();
            simpleTest.setId(baseId + 2);
//        simpleTest.setBooleanV(false);
            simpleTest.setByteV((byte) 89);
            simpleTest.setShortV((short) 169);
            simpleTest.setLongV(649);
            simpleTest.setStringO("Hello 999 !");
            simpleTest.setIntegerO(3209);
            simpleTest.setLongO(null);
            bufferedDBWriter.sendRecord(ownerId, simpleTest, OperationType.INSERT);

            long version = 1000;
            SortableDefinition sortableTest = new SortableDefinition();
            sortableTest.setId(baseId + 3);
            sortableTest.setVersion(version);
            sortableTest.setBooleanV(true);
            sortableTest.setByteV((byte) 8);
            sortableTest.setShortV((short) 16);
            sortableTest.setIntV(32);
            sortableTest.setLongV(64);
            sortableTest.setStringO("Hello World !");
            sortableTest.setIntegerO(320);
            sortableTest.setLongO(null);
            bufferedDBWriter.sendRecord(ownerId, sortableTest, OperationType.INSERT);

            sortableTest.setVersion(version + 3);
            for (int j = 0; j < 2; j++) {               // duplicated
                sortableTest.setByteV((byte) (8 * 3));
                sortableTest.setShortV((short) (16 * 3));
                sortableTest.setIntV(32 * 3);
                sortableTest.setLongV(64 * 3);
                sortableTest.setIntegerO(null);
                bufferedDBWriter.sendRecord(ownerId, sortableTest, OperationType.UPDATE);
            }

            sortableTest.setVersion(version + 1);
            sortableTest.setIntV(32 * 2);
            sortableTest.setLongV(64 * 2);
            sortableTest.setBooleanV(false);
            sortableTest.setLongO(640L);
            bufferedDBWriter.sendRecord(ownerId, sortableTest, OperationType.UPDATE);

            sortableTest.setVersion(version + 20);
            sortableTest.setLongO(0L);
            bufferedDBWriter.sendRecord(ownerId, sortableTest, OperationType.UPDATE);

            sortableTest.setVersion(version - 10);
            sortableTest.setLongO(6400L);
            bufferedDBWriter.sendRecord(ownerId, sortableTest, OperationType.UPDATE);
        }
    }
}
