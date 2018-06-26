# Trouble Shooting

## 1. 创建项目

Itellij IDEA 提供了Spring Initializer来初始化一个项目，通过勾选的方式配置pom.xml文件（对于我这种对xml不懂的新手来说太方便了）

### 常用配置

1. Web相关：Web中的web（涉及Spring MVC，RESTful Web Service等等）
2. 服务发现相关：Cloud Discovery 中Eureka Discovery 和Eureka Server（正常都勾选）
3. 数据库相关：SQL中的JPA、H2(默认使runtime，基于内存的数据库，不用配置，很方便)或MySQL(需要配置)

其他我也没用到orz



## 2. 用Postman测试文件上传、下载、删除

在测试阶段向后端发起请求时，推荐使用[Postman](https://www.getpostman.com),跨平台(win,mac,linux)、界面好看、简单易用，出错了也会有相对能看得懂的反馈（有更好用的也请告诉我）

### 上传文件

主要流程：

![流程](https://github.com/gs19980115/dfs-microservice/raw/master/namenode-frontend/images/upload-file.png)

### 下载文件

选择GET请求，输入网址

### 删除文件

选择DELETE请求，输入网址

## 3. 用RestTemplate在代码中发起Http请求

### 上传文件

```Java
// 设置上传的Url，注意要加“http://”
String url = "http://127.0.0.1:8080/"
    
// 设置param，很像Postman中设置Key和Value，选取文件
FileSystemResource resource = new FileSystemResource(new File(filePath));
MultiValueMap<String, Object> param = new LinkedMultiValueMap<>();
param.add("file", resource); 

// 发起Post请求上传文件
RestTemplate rest = new RestTemplate();

// Post 返回一个String类型的response,检查是否上传成功
String response = rest.postForObject(url, param, String.class);
```

### 删除文件

```Java
RestTemplate rest = new RestTemplate();
rest.delete(url);
```

### 下载文件

```Java
//获取资源（URLResource是获取网络资源，Spring封装的，可以直接用来下载文件）
UrlResource urlResource = new UrlResource(new URL(url));
InputStream inputStream = urlResource.getInputStream();

// 持久化到硬盘（不知道有么有别的方法）
Path blockPath = Paths.get(BLOCKS_DIRECTORY + blockUuid);
Files.copy(inputStream,blockPath, StandardCopyOption.REPLACE_EXISTING);
```



## 4. 后端接收文件

在Spring中上传下载文件，Spring官网里[Guides](https://spring.io/guides)中的[uploading-files例子](https://spring.io/guides/gs/uploading-files/)讲解的很好，例子也比较清晰明了（主要看FileUploadController.java和FileSystemStorageService.java）。
获取上传的资源文件，可以用@RequestParam注解，查阅一下手册即可,以下例子参考[uploading-files](https://spring.io/guides/gs/uploading-files/)

```java
    @PostMapping("/")
    public String handleFileUpload(@RequestParam("file") MultipartFile file,
            RedirectAttributes redirectAttributes) {

        storageService.store(file);
        redirectAttributes.addFlashAttribute("message",
                "You successfully uploaded " + file.getOriginalFilename() + "!");
        return "redirect:/";

    }
```

注意@RequestParam里的参数对应Postman中的Key值

## 5. 如何用URL传递参数

1. 提取URL中的简单信息(如从GET请求 /{directory}/{filename}中提取directory和filename)，可以用@PathVariable，查阅一下手册即可

```Java
@GetMapping("/{directory}/{filename}")
public void foo(@PathVariable("directory") String directory
                @PathVariable("filename") String filename){
	// directory 和 filename 就可以直接用了
}
```

2. 获取完整的http请求的链接

可以在Controller中声明一个HttpServletRequest

```Java
    // 注入方式获取前端发来的http请求
    @Autowired
    private HttpServletRequest request;
```

然后在@GetMapping、@PostMapping或@DeleteMapping的函数中可以使用，以Get http://127.0.0.1:8761/download/gaygay/gay2.txt 为例，说明相关函数功能

- request.getRequestURI() ———— /download/gaygay/gay2.txt
- request.getRequestURL() ————http://127.0.0.1:8761/download/gaygay/gay2.txt
- request.getServletPath() ——— — /download/gaygay/gay2.txt




## 6. 文件上传大小限制

- 问题描述：
 上传文件大小过大时，NameNode会抛出异常信息如下：

```
Spring Boot:The field file exceeds its maximum permitted size of 1048576 bytes.
```

- 解决方法，在application.properties中添加以下配置

```properties
# 调整上传文件大小限制
# 参考：https://blog.csdn.net/u010429286/article/details/54381705
spring.servlet.multipart.max-file-size = 10Mb
spring.servlet.multipart.max-request-size=100Mb
```

## 7. 跨域资源共享（CORS）

- 问题描述：
这个主要是前后端分离时，进行前后端交互时会出现的问题，调试前端时发现的，报错信息忘了，要么是500错误，要么会提示CORS字样。
- 解决方案

我是在NameNode中添加一个针对性的配置文件CorsConfig.java,可以解决，但似乎不优雅（有更好的方案可以告诉我）

```Java
@Configuration
public class CorsConfig extends WebMvcConfigurerAdapter {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins("*")
                .allowCredentials(true)
                .allowedMethods("GET", "POST", "DELETE", "PUT");
    }

}
```



## 8. Spring Data JPA数据库删改操作

- 问题描述：
我在Spring中利用了JPA和和数据库管理整个文件系统和DataNode、Block等信息，但在对数据库进行update、delete操作时会报异常，如下

```
No EntityManager with actual transaction available for current thread - cannot reliably process 'remove' call; nested exception is javax.persistence.TransactionRequiredException: No EntityManager with actual transaction available for current thread - cannot reliably process 'remove' call 
```

- 解决方案（参考：https://blog.csdn.net/hanghangde/article/details/53241150）

在Service层所有对数据删改的方法上添加@Transactional注解



## 9. 编码问题

- 问题描述

这个真的头疼，统一用UTF-8编码解码，后端需要对文件(名)用UTF-8解码，不然会出现乱码的问题

- 解决方案

这个问题我还没有完美的解决，以下解决方案只能做简单参考

```Java
    @PostMapping("/**")
    public ResponseEntity<?> uploadFile(@RequestParam("file") MultipartFile file) throws IOException {
        String iNodeName = request.getRequestURI() + file.getOriginalFilename();
        iNodeName = URLDecoder.decode(iNodeName, "UTF-8");
        return ResponseEntity.ok().build();
    }
```

主要通过了URLDecoder.decode(iNodeName, "UTF-8")把它转化成UTF-8编码。

## 10. 读取配置文件，配置Block大小和副本数

在application.properties中设置了默认参数（block大小和副本数）

```Pro
# 设置文件块大小,单位(B)
block.default-size=40000
# 设置默认副本数
block.default-replicas=2
```

在代码中可以通过**@Value**注解来注入，使用如下：

```Java
    // block 大小
    @Value(value="${block.default-size}")
    public int BLOCK_SIZE;

    // 副本数
    @Value(value="${block.default-replicas}")
    public int REPLICA_NUM;
```

