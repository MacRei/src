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
    }

}