private static void queryEvan() {
    RAImpl ra = new RAImpl();
    
    // Load the three relations
    Relation takes = new RelationBuilder()
            .attributeNames(List.of("ID", "course_id", "grade"))
            .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING))
            .build();
    takes.loadData("exported_data/takes.csv");
    
    Relation advisor = new RelationBuilder()
            .attributeNames(List.of("s_ID", "i_ID"))
            .attributeTypes(List.of(Type.STRING, Type.STRING))
            .build();
    advisor.loadData("exported_data/advisor.csv");
    
    Relation instructor = new RelationBuilder()
            .attributeNames(List.of("ID", "name"))
            .attributeTypes(List.of(Type.STRING, Type.STRING))
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
    Relation a_plus_stu_renamed = ra.rename(a_plus_stu, List.of("ID"), List.of("s_ID"))
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
}