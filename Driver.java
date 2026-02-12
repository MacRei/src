/**
 * Copyright (c) 2025 Sami Menik, PhD. All rights reserved.
 * 
 * Unauthorized copying of this file, via any medium, is strictly prohibited.
 * This software is provided "as is," without warranty of any kind.
 */
package uga.csx370.mydbimpl;

import java.util.List;

import uga.csx370.mydb.Predicate;
import uga.csx370.mydb.RA;
import uga.csx370.mydb.Relation;
import uga.csx370.mydb.RelationBuilder;
import uga.csx370.mydb.Type;

public class Driver {
    
    public static void main(String[] args) {
        // Following is an example of how to use the relation class.
        // This creates a table with three columns with below mentioned
        // column names and data types.
        // After creating the table, data is loaded from a CSV file.
        // Path should be replaced with a correct file path for a compatible
        // CSV file.

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

        System.out.println("Query 1: Find the names of departments with budget > 400000 that have courses being taught.");

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
        Relation result = ra.project(filtered, List.of("dept_name"));

        // Print result
        result.print();


    }

}
