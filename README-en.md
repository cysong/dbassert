# DbAssert

DbAssert is a simple and easy-to-use database assertion tool with complete test cases.

In automated testing, it is often necessary to verify the database after doing some operations or calling the interface.
DbAssert is designed for this purpose.

Features:

* Out of the box
* Supports Various types of assertion(details,aggregate,return data as list)
* Supports multiple data types, and compatible formats can be automatically converted
* Support paging query, sorting, etc.
* Support retry
* Support delay check
* Support to verify multiple type databases
* Easy extended report

## Installation

### Maven Project：

add dependency listed to your pom.xml file

```xml

<dependencies>
    <dependency>
        <groupId>com.github.cysong</groupId>
        <artifactId>dbassert</artifactId>
        <version>0.1-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## Usage

```java
Connection conn=DriverManager.getConnection("jdbc:sqlite::memory:");
        DbAssert.create(conn)
        .table("person")
        .where("id",1)
        .col("name").as("person name").isEqual("alice")
        .run();
```

More assertions see [Comparator](/src/main/java/com/github/cysong/dbassert/constant/Comparator.java)
，and more aggregate see [Aggregate](/src/main/java/com/github/cysong/dbassert/constant/Aggregate.java)

### ConnectionFactory

DbAssert supports ConnectionFactory to automatically generate connections based on database configuration files. The
default data configuration file is: `database.yml`, and the configuration format is as follows:

```yaml
- key: sqlite
  url: 'jdbc:sqlite::memory:'
- key: mysql
  driver: com.mysql.cj.jdbc.Driver
  url: jdbc:mysql://localhost:3306/dbassert?useUnicode=true&characterEncoding=utf-8&useSSL=true
  username: dbassert
  password: dbassert
```

call DbAssert like this with `ConnectionFactory` enabled

```java
DbAssert.create(mysql)
        .table("person")
        .where("id",1)
        .col("name").as("person name").isEqual("alice")
        .run();
```

The param `mysql` is the database key in `database.yml` file.

## Advanced Usage

### Global Configuration

If you want to change the default behavior of DbAssert, you can modify the global configuration (e.g. disable retries)：

```java
DbAssertSetup.setup()
        .retry(false)
        .delay(3000);
```

### Custom ConnectionFactory

You can implement `ConnectionFactory` and config like below：

```java
DbAssertSetup.setup()
        .factory(new MyConnectionFactory());
```

`MyConnectionFactory` is your custom `ConnectionFactory` implement

### Integrate Report

Dbassert provide interface `Reporter` to integrate test report，and implement `AllureReporter` for integrating Allure
report.How to integrate Allure report to your test project
see [Official Documents](https://docs.qameta.io/allure-report/)

You can copy `AllureReporter` to your project and config：

```java
DbAssertSetup.setup()
        .reporter(new AllureReporter());
```

You can also implement your own Reporter,and config the same way.

More usages see testcase: [DbAssertTest.java](/src/test/java/com/github/cysong/dbassert/DbAssertTest.java)
