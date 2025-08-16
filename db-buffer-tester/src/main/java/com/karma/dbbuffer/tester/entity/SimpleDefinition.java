package com.karma.dbbuffer.tester.entity;

import com.karma.dbbuffer.client.jctree.PField;
import com.karma.dbbuffer.client.jctree.PRecord;
import lombok.Getter;

@Getter
@PRecord(id = 1, name = "simple_definition", version = 2)
public class SimpleDefinition {

    @PField(0)
    private Long id;

    @PField(1)
    private boolean booleanV;

    @PField(2)
    private byte byteV;

    @PField(3)
    private short shortV;

    @PField(value = 4)
    private int intV;

    @PField(value = 5)
    private long longV;

    @PField(value = 6)
    private String stringO;

    @PField(value = 7)
    private Integer integerO;

    @PField(value = 8)
    private Long longO;

    public void setId(Long id) {
        this.id = id;
    }

    public void setBooleanV(boolean booleanV) {
        this.booleanV = booleanV;
    }

    public void setByteV(byte byteV) {
        this.byteV = byteV;
    }

    public void setShortV(short shortV) {
        this.shortV = shortV;
    }

    public void setIntV(int intV) {
        this.intV = intV;
    }

    public void setLongV(long longV) {
        this.longV = longV;
    }

    public void setStringO(String stringO) {
        this.stringO = stringO;
    }

    public void setIntegerO(Integer integerO) {
        this.integerO = integerO;
    }

    public void setLongO(Long longO) {
        this.longO = longO;
    }
}
