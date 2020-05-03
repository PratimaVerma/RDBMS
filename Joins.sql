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

mysql> select IDA.ID AS IDA, IDB.ID AS IDB from IDA inner join IDB on IDA.ID=IDB.ID;
+------+------+
| IDA  | IDB  |
+------+------+
|    1 |    1 |
|    1 |    1 |
|    1 |    1 |
+------+------+

mysql> select IDA.ID AS IDA, IDB.ID AS IDB from IDA LEFT join IDB on IDA.ID=IDB.ID;
+------+------+
| IDA  | IDB  |
+------+------+
|    1 |    1 |
|    1 |    1 |
|    1 |    1 |
+------+------+

mysql> select IDA.ID AS IDA, IDB.ID AS IDB from IDA RIGHT join IDB on IDA.ID=IDB.ID;
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
