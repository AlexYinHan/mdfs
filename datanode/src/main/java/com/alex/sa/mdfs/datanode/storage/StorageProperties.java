package com.alex.sa.mdfs.datanode.storage;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("storage")
public class StorageProperties {

    /**
     * Folder location for storing files
     */

//    private String location = "upload-dir";
    @Value(value = "${server.port}")
    private Integer location;

    public String getLocation() {
        return location.toString();
    }

//    public void setLocation(String location) {
//        this.location = location;
//    }

}
