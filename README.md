# DbAssert

DbAssert是一个简单、易使用的数据库校验工具，有完善的测试用例。

在做自动化测试过程中，做一些操作或者调用接口之后经常需要校验数据库，DbAssert正是为这个目的而设计的。

DbAssert具有以下特点：

* 开箱即用
* 校验类型丰富，除了明细数据也支持校验汇总数据，或者将返回结果作为list整体校验
* 支持多种数据类型，并且兼容的格式可以自动转换
* 支持分页查询，排序等
* 支持重试
* 支持延迟校验
* 支持同时校验多种数据库
* 报告易扩展

## 安装

### maven项目：

pom文件增加以下依赖

```xml

<dependencies>
    <dependency>
        <groupId>com.github.cysong</groupId>
        <artifactId>dbassert</artifactId>
        <version>0.1-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## 基本使用

```java
Connection conn=DriverManager.getConnection("jdbc:sqlite::memory:");
        DbAssert.create(conn)
        .table("person")
        .where("id",1)
        .col("name").as("person name").isEqual("alice")
        .run();
```
更多校验类型见[Comparator](/src/main/java/com/github/cysong/dbassert/constant/Comparator.java)
，更多的汇总方式见[Aggregate](/src/main/java/com/github/cysong/dbassert/constant/Aggregate.java)

### 使用ConnectionFactory

DbAssert支持ConnectionFactory自动根据数据库配置文件创建连接，默认的数据配置文件为：`database.yml`，配置格式如下：

```yaml
- key: sqlite
  url: 'jdbc:sqlite::memory:'
- key: mysql
  driver: com.mysql.cj.jdbc.Driver
  url: jdbc:mysql://localhost:3306/dbassert?useUnicode=true&characterEncoding=utf-8&useSSL=true
  username: dbassert
  password: dbassert
```

使用`ConnectionFactory`时需要这样调用DbAssert：

```java
DbAssert.create(mysql)
        .table("person")
        .where("id",1)
        .col("name").as("person name").isEqual("alice")
        .run();
```

其中`mysql`为`database.yml`中配置的数据库`key`。

## 高级用法

### 全局配置

如果要改变DbAssert的默认行为，可以修改全局配置（例如禁用重试）：

```java
DbAssertSetup.setup()
        .retry(false)
        .delay(3000);
```

### 自定义ConnectionFactory

如果默认的`ConnectionFactory`无法满足需求，也可以实现自己的`ConnectionFactory`，然后配置如下：

```java
DbAssertSetup.setup()
        .factory(new MyConnectionFactory());
```

其中`MyConnectionFactory`是你自己的`ConnectionFactory`实现

### 集成报告

系统提供了报告接口`Reporter`，并提供了集成Allure报告的实现`AllureReporter`。Allure report的集成方式参见[官方文档](https://docs.qameta.io/allure-report/)

要集成allure报告，首先你需要在自己的测试项目中集成`Allure report`，然后将`AllureReporter`拷贝到自己的项目下并配置：

```java
DbAssertSetup.setup()
        .reporter(new AllureReporter());
```

如果`AllureReporter`无法满足需求，你需要实现自己的Reporter，并按照以上方式配置

更多用法参考测试用例：[DbAssertTest.java](/src/test/java/com/github/cysong/dbassert/DbAssertTest.java)
