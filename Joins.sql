Create Below Table in Hive:

create table IDA(
ID INT
);

insert into IDA(ID) values(1);
insert into IDA(ID) values(1);
insert into IDA(ID) values(1);


create table IDB(
ID INT
);

insert into IDB(ID) values(1);

select IDA.ID AS IDA, IDB.ID AS IDB from IDA inner join IDB on IDA.ID=IDB.ID;
+------+------+
| IDA  | IDB  |
+------+------+
|    1 |    1 |
|    1 |    1 |
|    1 |    1 |
+------+------+

select IDA.ID AS IDA, IDB.ID AS IDB from IDA LEFT join IDB on IDA.ID=IDB.ID;
+------+------+
| IDA  | IDB  |
+------+------+
|    1 |    1 |
|    1 |    1 |
|    1 |    1 |
+------+------+

select IDA.ID AS IDA, IDB.ID AS IDB from IDA RIGHT join IDB on IDA.ID=IDB.ID;
+------+------+
| IDA  | IDB  |
+------+------+
|    1 |    1 |
|    1 |    1 |
|    1 |    1 |
+------+------+



create table Emp(
eid INT,
ename String,
deptno String
);

insert into Emp(eid, ename, deptno) values(1, 'A', 'D1');
insert into Emp(eid, ename, deptno) values(2, 'B', 'D2');
insert into Emp(eid, ename, deptno) values(3, 'C', NULL);
insert into Emp(eid, ename, deptno) values(4, 'D', 'D6');
insert into Emp(eid, ename, deptno) values(5, 'E', NULL);
insert into Emp(eid, ename, deptno) values(6, 'F', 'D3');


create table dept(
deptno String,
dname String
);

insert into dept(deptno, dname) values('D1', 'XYZ');
insert into dept(deptno, dname) values('D2', 'MNO');
insert into dept(deptno, dname) values('D4', 'PQR');
insert into dept(deptno, dname) values('D5', 'WXY');

---------
spark.table("rajeshkr.IDA").createOrReplaceTempView("IDA")
spark.table("rajeshkr.IDB").createOrReplaceTempView("IDB")
spark.table("rajeshkr.Emp").createOrReplaceTempView("Emp")
spark.table("rajeshkr.dept").createOrReplaceTempView("dept")

scala> sql("select IDA.ID AS ID_A, IDB.ID AS ID_B from IDA inner join IDB on IDA.ID=IDB.ID").show
+----+----+
|ID_A|ID_B|
+----+----+
|   1|   1|
|   1|   1|
|   1|   1|
+----+----+

scala> sql("select IDA.ID AS ID_A, IDB.ID AS ID_B from IDA left join IDB on IDA.ID=IDB.ID").show
+----+----+
|ID_A|ID_B|
+----+----+
|   1|   1|
|   1|   1|
|   1|   1|
+----+----+

scala> sql("select IDA.ID AS ID_A, IDB.ID AS ID_B from IDA right join IDB on IDA.ID=IDB.ID").show
+----+----+
|ID_A|ID_B|
+----+----+
|   1|   1|
|   1|   1|
|   1|   1|
+----+----+

scala> sql("select IDA.ID AS ID_A, IDB.ID AS ID_B from IDA full join IDB on IDA.ID=IDB.ID").show
+----+----+
|ID_A|ID_B|
+----+----+
|   1|   1|
|   1|   1|
|   1|   1|
+----+----+



scala> spark.sql("select * from dept").show
+------+-----+
|deptno|dname|
+------+-----+
|    D1|  XYZ|
|    D2|  MNO|
|    D4|  PQR|
|    D5|  WXY|
+------+-----+


scala> spark.sql("select * from emp").show
+---+-----+------+
|eid|ename|deptno|
+---+-----+------+
|  1|    A|    D1|
|  2|    B|    D2|
|  3|    C|  null|
|  4|    D|    D6|
|  5|    E|  null|
|  6|    F|    D3|
+---+-----+------+


scala> spark.sql("select e.eid,e.ename, e.deptno, d.deptno, d.dname from emp e inner join dept d on e.deptno=d.deptno").show
+---+-----+------+------+-----+
|eid|ename|deptno|deptno|dname|
+---+-----+------+------+-----+
|  1|    A|    D1|    D1|  XYZ|
|  2|    B|    D2|    D2|  MNO|
+---+-----+------+------+-----+

scala> spark.sql("select e.eid,e.ename, e.deptno, d.deptno, d.dname from emp e left join dept d on e.deptno=d.deptno").show
+---+-----+------+------+-----+
|eid|ename|deptno|deptno|dname|
+---+-----+------+------+-----+
|  1|    A|    D1|    D1|  XYZ|
|  2|    B|    D2|    D2|  MNO|
|  3|    C|  null|  null| null|
|  4|    D|    D6|  null| null|
|  5|    E|  null|  null| null|
|  6|    F|    D3|  null| null|
+---+-----+------+------+-----+


scala> spark.sql("select e.eid,e.ename, e.deptno, d.deptno, d.dname from emp e left join dept d on e.deptno=d.deptno where e.deptno is null").show
+---+-----+------+------+-----+
|eid|ename|deptno|deptno|dname|
+---+-----+------+------+-----+
|  3|    C|  null|  null| null|
|  5|    E|  null|  null| null|
+---+-----+------+------+-----+


scala> spark.sql("select e.eid,e.ename, e.deptno, d.deptno, d.dname from emp e left join dept d on e.deptno=d.deptno where d.deptno is null").show
+---+-----+------+------+-----+
|eid|ename|deptno|deptno|dname|
+---+-----+------+------+-----+
|  3|    C|  null|  null| null|
|  4|    D|    D6|  null| null|
|  5|    E|  null|  null| null|
|  6|    F|    D3|  null| null|
+---+-----+------+------+-----+


scala> spark.sql("select e.eid,e.ename, e.deptno, d.deptno, d.dname from emp e right join dept d on e.deptno=d.deptno").show
+----+-----+------+------+-----+
| eid|ename|deptno|deptno|dname|
+----+-----+------+------+-----+
|   1|    A|    D1|    D1|  XYZ|
|   2|    B|    D2|    D2|  MNO|
|null| null|  null|    D4|  PQR|
|null| null|  null|    D5|  WXY|
+----+-----+------+------+-----+

scala> spark.sql("select e.eid,e.ename, e.deptno, d.deptno, d.dname from emp e full join dept d on e.deptno=d.deptno").show
+----+-----+------+------+-----+
| eid|ename|deptno|deptno|dname|
+----+-----+------+------+-----+
|null| null|  null|    D5|  WXY|
|   3|    C|  null|  null| null|
|   5|    E|  null|  null| null|
|   1|    A|    D1|    D1|  XYZ|
|   6|    F|    D3|  null| null|
|   2|    B|    D2|    D2|  MNO|
|null| null|  null|    D4|  PQR|
|   4|    D|    D6|  null| null|
+----+-----+------+------+-----+























