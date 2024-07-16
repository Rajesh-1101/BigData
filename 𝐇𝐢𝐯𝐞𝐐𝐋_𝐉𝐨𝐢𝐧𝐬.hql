// 𝐄𝐱𝐩𝐥𝐨𝐫𝐢𝐧𝐠 𝐇𝐢𝐯𝐞𝐐𝐋 𝐉𝐨𝐢𝐧𝐬: 𝐀 𝐐𝐮𝐢𝐜𝐤 𝐆𝐮𝐢𝐝𝐞!
===============================

// The HiveQL Join clause is a powerful tool used to combine data from two or more tables based on a related column. Here are the various types of HiveQL joins you can use:

// 𝐈𝐧𝐧𝐞𝐫 𝐉𝐨𝐢𝐧 
// The HiveQL inner join returns rows from multiple tables where the join condition is met. This means only matching records in every table being joined are returned.


SELECT T1.empname, T2.department_name 
FROM employee T1 
JOIN employee_department T2 
ON T1.empid = T2.depid;


// 𝐋𝐞𝐟𝐭 𝐎𝐮𝐭𝐞𝐫 𝐉𝐨𝐢𝐧
// The HiveQL left outer join returns all records from the left (first) table and only matching records from the right (second) table.


SELECT e1.empname, e2.department_name 
FROM employee e1 
LEFT OUTER JOIN employee_department e2 
ON e1.empid = e2.depid;


// 𝐑𝐢𝐠𝐡𝐭 𝐎𝐮𝐭𝐞𝐫 𝐉𝐨𝐢𝐧
//The HiveQL right outer join returns all records from the right (second) table and only matching records from the left (first) table.


SELECT e1.empname, e2.department_name 
FROM employee e1 
RIGHT OUTER JOIN employee_department e2 
ON e1.empid = e2.depid;


// 𝐅𝐮𝐥𝐥 𝐎𝐮𝐭𝐞𝐫 𝐉𝐨𝐢𝐧 
//The HiveQL full outer join returns all records from both tables, assigning NULL to missing records in either table.

SELECT e1.empname, e2.department_name 
FROM employee e1 
FULL OUTER JOIN employee_department e2 
ON e1.empid = e2.depid;

