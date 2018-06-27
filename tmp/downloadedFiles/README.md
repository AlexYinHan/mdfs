# 基于微服务架构的分布式文件系统

## 项目简介

模拟HDFS，基于Spring Boot和Spring Cloud Eureka实现分布式文件系统，同时，基于Vue.js和Element UI提供NameNode上的web前端界面以供用户交互。

## 使用说明

### 1. 启动NameNode

```shell
$ cd namenode
$ ./mvnw spring-boot:run
```

NameNode默认在8761端口启动

可以在application.properties中修改配置，默认配置如下

```properties
# 设置文件块(Block)的大小,单位(B)
block.default-size=40000
# 设置默认副本数
block.default-replicas=2
```

### 2. 启动多个DataNode实例

```Shell
$ cd datanode
$ ./mvnw spring-boot:run -Dserver.port=8082
$ ./mvnw spring-boot:run -Dserver.port=8084
$ ./mvnw spring-boot:run -Dserver.port=8086
```


### 3. 启动NameNode上的前端

```Shell
$ cd namenode-frontend
$ npm install
$ npm run dev
```

NameNode 上的前端默认占用8080端口启动

## 项目截图
![list](https://github.com/gs19980115/dfs-microservice/raw/master/namenode-frontend/images/list.png)
![grid](https://github.com/gs19980115/dfs-microservice/raw/master/namenode-frontend/images/grid.png)
![服务下线，数据迁移之前](https://github.com/gs19980115/dfs-microservice/raw/master/namenode-frontend/images/before-migration.png)
![数据迁移后](https://github.com/gs19980115/dfs-microservice/raw/master/namenode-frontend/images/after-migration.png)


