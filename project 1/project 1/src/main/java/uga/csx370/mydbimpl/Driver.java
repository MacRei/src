/**
 * Copyright (c) 2025 Sami Menik, PhD. All rights reserved.
 * 
 * Unauthorized copying of this file, via any medium, is strictly prohibited.
 * This software is provided "as is," without warranty of any kind.
 */
package uga.csx370.mydbimpl;

import java.util.List;

import uga.csx370.mydb.Relation;
import uga.csx370.mydb.RelationBuilder;
import uga.csx370.mydb.Type;
import uga.csx370.mydb.*;
import uga.csx370.mydbimpl.RAImpl;

public class Driver {
    
    public static void main(String[] args) {
        
        // myID
        System.out.println("myid: 811705713");
        // relation 1 (instructor table)
        Relation rel1 = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "salary"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        // load data from CSV file
        rel1.loadData("exported_data/instructor_export.csv"); //instructor_export_small if only want first 10 entries
        // print the relation
        rel1.print();

        // myID
        System.out.println("myid: 811705713");
        // relation 2 (student table)
        Relation rel2 = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        // load data from CSV file
        rel2.loadData("exported_data/student_export.csv"); //student_export_small if only want first 10 entries
        // print the relation
        rel2.print();

/*
Interesting Query: Names and ids of advisors of students who received an A+ in any course
Similar to example from class (below):
        Need → ID, course_id, year, sec_id, semester, grade
        Tables → advisor, instructor, takes
                gt50 = SELECT[grade > 50] (Takes)
                gt50_stu_id = PROJECT[ID] (gt50)
                gt50_adv = gt50_stu_id RENAME[ID → s_ID] JOIN Advisor
                adv_id = PROJECT[i_ID] (gt50_adv)
                adv_info = adv_id RENAME[i_ID → ID] JOIN Instructor
                PROJECT[ID, name] (adv_info)

SQL:
-- It took a while to figure out the syntax. 
-- I had to look up subqueries and JOIN syntax since it was different from class example.
-- Also grade is by letter, not int, so had to change that as well.
-- second attempt (first attempt was the same but without SELECT * FROMM (), JUST (..) and AS name)
-- had to look up SELECT * FROM () syntax. Basically subqueries.
-- had to figure out the JOIN syntax as well as it is different from class
SELECT * FROM (
SELECT DISTINCT ID, name
from instructor
) AS instr,
SELECT * FROM (
SELECT ID, grade
from takes
WHERE grade = 'A+'
) AS a_tch,
SELECT * FROM (
SELECT DISTINCT ID
from student
) AS stu,
JOIN stu ON a_tch.ID = stu.ID
AS adv
JOIN stu ON adv.s_ID = stu.ID
AS adv_a
JOIN adv_a ON instr = adv_a.i_ID
AS adv_i
PROJECT[ID, name] (adv_i)

-- Successful attempt:
-- had to look up WITH name AS ()
-- assumption: takes ID and student ID are the same. Seems the most likely
-- correct assumption because below code returns empty set:
                SELECT DISTINCT t.ID
                FROM takes t
                LEFT JOIN student s ON t.ID = s.ID
                WHERE s.ID IS NULL;

-- Step 1: Get student IDs who received an A+ in any course
WITH a_tch AS (
    SELECT DISTINCT ID
    FROM takes
    WHERE grade = 'A+'
),
-- Step 2: Find the advisor (instructor ID) for each of those students
adv AS (
    SELECT DISTINCT a.i_ID              -- gets distinct advisor IDs
    FROM a_tch                          -- students with A+
    JOIN advisor a ON a_tch.ID = a.s_ID -- Join ID from a_tch (student IDs w/ A+) with s_ID from advisor. This gives us all advisors of A+ students
)
-- Step 3: Get the advisor's name and ID from the instructor table
SELECT DISTINCT i.ID, i.name
FROM adv
JOIN instructor i ON adv.i_ID = i.ID

-- Step 4: Export the result to a CSV file
INTO OUTFILE '/var/lib/mysql-files/query_export_evan.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

exit;

docker cp 1e6e538a6f28:/var/lib/mysql-files/query_export_evan.csv ./exported_data/query_export_evan.csv

*/
    }

}

// what I used to run the driver class:
// I think my maven may have installed wrong? Unsure, will have to look into it.
// It is correct in MobaXTerm, I just do not code in there and did not want to connect container to it somehow.
// mvn clean compile exec:java "-Dexec.mainClass=uga.csx370.mydbimpl.Driver"
// mvn exec:java "-Dexec.mainClass=uga.csx370.mydbimpl.Driver"