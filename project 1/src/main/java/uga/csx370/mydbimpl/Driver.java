/**
 * Copyright (c) 2025 Sami Menik, PhD. All rights reserved.
 * 
 * Unauthorized copying of this file, via any medium, is strictly prohibited.
 * This software is provided "as is," without warranty of any kind.
 */
package uga.csx370.mydbimpl;

import java.util.List;

import uga.csx370.mydb.*;
import uga.csx370.mydbimpl.RAImpl;

public class Driver {
    
    public static void main(String[] args) {
        
        //// myID
        //System.out.println("myid: 811705713");
        //// relation 1 (instructor table)
        //Relation rel1 = new RelationBuilder()
        //        .attributeNames(List.of("ID", "name", "dept_name", "salary"))
        //        .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.DOUBLE))
        //        .build();
        //// load data from CSV file
        //rel1.loadData("exported_data/instructor_export.csv"); //instructor_export_small if only want first 10 entries
        //// print the relation
        //rel1.print();
//
        //// myID
        //System.out.println("myid: 811705713");
        //// relation 2 (student table)
        //Relation rel2 = new RelationBuilder()
        //        .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
        //        .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
        //        .build();
        //// load data from CSV file
        //rel2.loadData("exported_data/student_export.csv"); //student_export_small if only want first 10 entries
        //// print the relation
        //rel2.print();
//


        RA ra = new RAImpl();
        

        Relation rel1 = new RelationBuilder()
                .attributeNames(List.of("courseID", "name", "dept_name", "credits"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        rel1.loadData("exported_data/course.csv");

        Relation rel2 = new RelationBuilder()
        .attributeNames(List.of("course_ID", "prereq_ID"))
        .attributeTypes(List.of(Type.STRING, Type.STRING))
        .build();
        rel2.loadData("exported_data/prereq.csv");
        Relation rel2Limited = new RelationBuilder()
                .attributeNames(rel2.getAttrs())
                .attributeTypes(rel2.getTypes())
                .build();
        int rows = Math.min(50, rel2.getSize());
        for (int i = 0; i < rows; i++) {
            rel2Limited.insert(rel2.getRow(i));
        }

        Relation joined = ra.join(rel1, rel2, row ->
                row.get(0).toString().equals(row.get(4).toString())
        );

        joined = ra.project(joined, List.of("course_ID", "name", "prereq_ID"));
        joined = ra.rename(joined, List.of("course_ID", "name", "prereq_ID"), List.of("course_ID", "courseName", "prereq_ID"));
        joined = ra.join(joined, rel1, row ->
                row.get(2).toString().equals(row.get(3).toString())
        );
        joined = ra.project(joined, List.of("course_ID", "courseName", "prereq_ID", "name"));
        joined = ra.rename(joined, List.of("course_ID", "courseName", "prereq_ID", "name"), List.of("course_ID", "course_name", "prereq_ID", "prereq_name"));
        joined.print();
       
        // Evan's query
        System.out.println("myid: 811705719");
        queryEvan();
        queryPhysicsStudents();
        queryNate();
        queryAdam();
    }

    // Evan's query
    // Gets advisors of A+ studnets and puts the advisors ID and name in query_evan.csv
    private static void queryEvan() {
        RAImpl ra = new RAImpl();
        
        // Load the three relations needed for the query
        Relation takes = new RelationBuilder()
                .attributeNames(List.of("ID", "course_id", "sec_id", "semester", "year", "grade"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING))
                .build();
        takes.loadData("exported_data/takes.csv");
        
        Relation advisor = new RelationBuilder()
                .attributeNames(List.of("s_ID", "i_ID"))
                .attributeTypes(List.of(Type.STRING, Type.STRING))
                .build();
        advisor.loadData("exported_data/advisor.csv");
        
        Relation instructor = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "salary"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        instructor.loadData("exported_data/instructor.csv");
        
        // Step 1: Get student IDs who received an A+ in any course
        // use lamba expression to sort through and find all A+ students
        Relation a_plus = ra.select(takes, row -> {
            String grade = row.get(takes.getAttrIndex("grade")).toString();
            return grade.equals("A+");
        });
        // takes A+ students and gets just the ID of each A+ student
        Relation a_plus_stu = ra.project(a_plus, List.of("ID"));
        
        // change a_plus_stu ID to s_ID so it works with advisor table
        Relation a_plus_stu_renamed = ra.rename(a_plus_stu, List.of("ID"), List.of("s_ID"));
        // Step 2: Find the advisor (instructor ID) for each of those students
        Relation a_plus_adv = ra.join(a_plus_stu_renamed, advisor);

        // no longer need s_ID
        Relation adv_ids = ra.project(a_plus_adv, List.of("i_ID"));
        
        // Step 3: Get the advisor's name and ID from the instructor table
        // Makes advisor i_ID --> advisor ID. Now advisor "ID === instructor "ID" syntax.
        Relation adv_ids_renamed = ra.rename(adv_ids, List.of("i_ID"), List.of("ID"));
        // get the advisors from instructor table with there ID and name
        Relation adv = ra.join(adv_ids_renamed, instructor);
        Relation adv_of_a_plus_stu = ra.project(adv, List.of("ID", "name"));
        
        // Print the result (advisors of A+ students)
        adv_of_a_plus_stu.print();

        // Export the result to a CSV file
        // adv_of_a_plus_stu.loadData("query_evan.csv");
    }

    //Kayla
    private static void queryPhysicsStudents() {
        RAImpl ra = new RAImpl();

        //Load student table
        Relation student = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        student.loadData("exported_data/student.csv");

        //Load takes table
        Relation takes = new RelationBuilder()
                .attributeNames(List.of("ID", "course_id", "sec_id", "semester", "year", "grade"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING))
                .build();
        takes.loadData("exported_data/takes.csv");

        //Load course table
        Relation course = new RelationBuilder()
                .attributeNames(List.of("courseID", "name", "dept_name", "credits"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        course.loadData("exported_data/course.csv");

        //Filter Physics courses
        Relation physicsCourses = ra.select(course, row ->
                row.get(course.getAttrIndex("dept_name")).toString().equalsIgnoreCase("Physics")
        );

        //Join takes
        Relation studentTakesPhysics = ra.join(takes, physicsCourses, row -> {
            int takesCourseIndex = takes.getAttrIndex("course_id");
            int courseIdIndex = takes.getAttrs().size() + physicsCourses.getAttrIndex("courseID");
            return row.get(takesCourseIndex).toString().equals(row.get(courseIdIndex).toString());
        });

        //Rename course
        Relation studentTakesPhysicsRenamed = ra.rename(studentTakesPhysics,
                List.of("name"), List.of("courseName"));

        //Join with student table on student ID
        Relation studentsPhysicsInfo = ra.join(studentTakesPhysicsRenamed, student, row -> {
            int takesIdIndex = studentTakesPhysicsRenamed.getAttrIndex("ID");
            int studentIdIndex = studentTakesPhysicsRenamed.getAttrs().size() + student.getAttrIndex("ID");
            return row.get(takesIdIndex).toString().equals(row.get(studentIdIndex).toString());
        });

        //Project only student ID, student name, and course name
        Relation result = ra.project(studentsPhysicsInfo, List.of("ID", "name", "courseName"));


        Relation limitedResult = new RelationBuilder()
                .attributeNames(result.getAttrs())
                .attributeTypes(result.getTypes())
                .build();
        int rowsToShow = Math.min(15, result.getSize());
        for (int i = 0; i < rowsToShow; i++) {
            limitedResult.insert(result.getRow(i));
        }

        System.out.println("\nQuery: Students taking Physics courses");
        limitedResult.print();

    }

    // Nate's query
    // Gets the names of departments with budget > 400000 that have courses being taught.
    private static void queryNate() {
        System.out.println("811887600");

        Relation instructor = new RelationBuilder()
                .attributeNames(List.of("id", "name", "dept_name", "salary"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        instructor.loadData("/mysql-data-x370/activity02DBM/instructor_export.csv");
        //instructor.print();
        Relation teaches = new RelationBuilder()
                .attributeNames(List.of("id", "course_id", "sec_id", "semester", "year"))
                .attributeTypes(List.of(
                        Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        teaches.loadData("/mysql-data-x370/activity02DBM/teaches_export.csv");

        Relation course = new RelationBuilder()
                .attributeNames(List.of("course_id", "title", "dept_name", "credits"))
                .attributeTypes(List.of(
                        Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        course.loadData("/mysql-data-x370/activity02DBM/course_export.csv");

        Relation department = new RelationBuilder()
                .attributeNames(List.of("dept_name", "building", "budget"))
                .attributeTypes(List.of(
                        Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        department.loadData("/mysql-data-x370/activity02DBM/department_export.csv");

        System.out.println("Query 1: Find the names of departments with budget > 400000 and display their courses being taught.");

        RA ra = new RAImpl();

        // department ⨝ course
        Relation deptCourse = ra.join(department, course);

        // (department ⨝ course) ⨝ teaches
        Relation fullJoin = ra.join(deptCourse, teaches);

        // SELECT budget > 400000
        int budgetIndex = fullJoin.getAttrs().indexOf("budget");

        Predicate budgetPredicate = row ->
                row.get(budgetIndex).getAsDouble() > 700000;

        Relation filtered = ra.select(fullJoin, budgetPredicate);

        // PROJECT dept_name
        Relation result = ra.project(filtered, List.of("dept_name", "budget","title"));

        // Print result
        result.print();
    }

    // Adam's query
    // Find pairs of departments that share a prerequisite: one dept has a course
    // that requires a prereq offered by a different department.
    private static void queryAdam() {
        System.out.println("myid: 811354899");
        System.out.println("Query: Pairs of departments that share a prerequisite course");

        RA ra = new RAImpl();

        // Load course table
        Relation course = new RelationBuilder()
                .attributeNames(List.of("courseID", "name", "dept_name", "credits"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        course.loadData("exported_data/course.csv");

        // Load prereq table
        Relation prereq = new RelationBuilder()
                .attributeNames(List.of("course_ID", "prereq_ID"))
                .attributeTypes(List.of(Type.STRING, Type.STRING))
                .build();
        prereq.loadData("exported_data/prereq.csv");

        // Rename course attrs for first join (course that HAS the prereq)
        Relation courseRenamed1 = ra.rename(course,
                List.of("courseID", "name", "dept_name", "credits"),
                List.of("req_courseID", "req_name", "dept_requiring", "req_credits"));

        // Join prereq with courseRenamed1 on course_ID = req_courseID
        Relation withRequiring = ra.join(prereq, courseRenamed1, row ->
                row.get(0).toString().equals(row.get(2).toString())
        );

        // Project to just course_ID, prereq_ID, dept_requiring
        Relation reqDept = ra.project(withRequiring, List.of("course_ID", "prereq_ID", "dept_requiring"));

        // Rename course attrs for second join (course that IS the prereq)
        Relation courseRenamed2 = ra.rename(course,
                List.of("courseID", "name", "dept_name", "credits"),
                List.of("pre_courseID", "pre_name", "dept_offering_prereq", "pre_credits"));

        // Join reqDept with courseRenamed2 on prereq_ID = pre_courseID
        Relation withOffering = ra.join(reqDept, courseRenamed2, row ->
                row.get(1).toString().equals(row.get(3).toString())
        );

        // Select only pairs where departments are different
        Relation diffDepts = ra.select(withOffering, row -> {
            int reqIdx = withOffering.getAttrIndex("dept_requiring");
            int offIdx = withOffering.getAttrIndex("dept_offering_prereq");
            return !row.get(reqIdx).toString().equals(row.get(offIdx).toString());
        });

        // Project to just the two department names
        Relation result = ra.project(diffDepts, List.of("dept_requiring", "dept_offering_prereq"));

        result.print();
    }
}
