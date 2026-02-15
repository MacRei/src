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
        rel1.loadData("project 1/project 1/exported_data/course.csv");

        Relation rel2 = new RelationBuilder()
        .attributeNames(List.of("course_ID", "prereq_ID"))
        .attributeTypes(List.of(Type.STRING, Type.STRING))
        .build();
        rel2.loadData("project 1/project 1/exported_data/prereq.csv");

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
    }
    
}