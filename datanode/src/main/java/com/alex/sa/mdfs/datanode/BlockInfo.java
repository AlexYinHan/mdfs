package com.alex.sa.mdfs.datanode;


import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class BlockInfo {
    @Id
    @GeneratedValue
    private Long id;

    public String getFileName() {
        return fileName;
    }

    private String fileName;

    public long getSize() {
        return size;
    }

    private long size;

    public BlockInfo(String fileName, long size) {
        this.fileName = fileName;
        this.size = size;
    }
}
