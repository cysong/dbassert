create table customer
(
    id   int         not null,
    name varchar(16) not null
);

insert into customer(id, name)
values (1, "alice"),
       (2, "bob"),
       (3, "cal");