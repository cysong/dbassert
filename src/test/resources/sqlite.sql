create table person
(
    id     int         not null PRIMARY KEY,
    name   varchar(16) not null,
    age    int null,
    weight float null,
    height double null
);

insert into person(id, name, age, weight, height)
values (1, "alice", 10, 40.0, 94.5),
       (2, "bob", 20, 55.5, null),
       (3, "cal", null, null, 172.4);