create table person
(
    id     int         not null PRIMARY KEY,
    name   varchar(16) not null,
    gender char(1) null,
    age    int null,
    weight float null,
    height double null
);

insert into person(id, name, gender, age, weight, height)
values (1, "alice", 'F', 10, 40.0, 94.0),
       (2, "bob", 'M', 20, 55.5, null),
       (3, "cal", 'M', null, null, 172.4);