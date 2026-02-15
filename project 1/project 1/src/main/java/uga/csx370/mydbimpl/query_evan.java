public static void main(String[] args) {
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
    Relation a_plus = ra.select(takes, row -> {
        String grade = row.get(takes.getAttrIndex("grade")).toString();
        return grade.equals("A+");
    });
    Relation a_plus_stu = ra.project(a_plus, List.of("ID"));
    
    // Step 2: Find the advisor (instructor ID) for each of those students
    Relation a_plus_adv = ra.join(a_plus_stu, advisor);
    Relation adv_ids = ra.project(a_plus_adv, List.of("i_ID"));
    
    // Step 3: Get the advisor's name and ID from the instructor table
    // Rename i_ID to ID so it matches instructor's ID for the join
    Relation adv = ra.join(adv_ids, instructor);
    Relation final_result = ra.project(adv, List.of("ID", "name"));
    
    // Print the result
    final_result.print();
}