# DbAssert

DbAssert是一个简单、易使用的数据库校验工具，不包括任何第三方依赖

## 安装

### maven项目：

```xml
<dependencies>
    <dependency>
        <groupId>com.github.cysong</groupId>
        <artifactId>dbassert</artifactId>
        <version>0.1-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## 使用

```java
Connection conn = DriverManager.getConnection("jdbc:sqlite:sqlite.db");
DbAssert.create(conn)
        .table("customer")
        .where("id", 1)
        .rowsEqual(1)
        .col("name").as("customer name").isEqual("alice")
        .run();
```


