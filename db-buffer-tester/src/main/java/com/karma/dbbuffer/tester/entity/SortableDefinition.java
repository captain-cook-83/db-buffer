package com.karma.dbbuffer.tester.entity;

import com.karma.dbbuffer.client.jctree.PField;
import com.karma.dbbuffer.client.jctree.PRecord;
import lombok.Getter;

@Getter
@PRecord(id = 2, name = "sortable_definition", version = 2, sortingOrder = true)
public class SortableDefinition {

    @PField(0)
    private Long id;

    @PField(1)
    private boolean booleanV;

    @PField(2)
    private byte byteV;

    @PField(3)
    private short shortV;

    @PField(4)
    private int intV;

    @PField(5)
    private long longV;

    @PField(6)
    private String stringO;

    @PField(7)
    private Integer integerO;

    @PField(8)
    private Long longO;

    @PField(9)
    private long version;

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

    public void setVersion(long version) { this.version = version; }
}
