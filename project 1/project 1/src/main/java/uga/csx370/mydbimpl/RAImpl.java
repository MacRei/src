package uga.csx370.mydbimpl;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import uga.csx370.mydb.Predicate;
import uga.csx370.mydb.RA;
import uga.csx370.mydb.Relation;
import uga.csx370.mydb.RelationBuilder;
import uga.csx370.mydb.Cell;
import uga.csx370.mydb.Type;


public class RAImpl implements RA {

    // need to do
    public Relation select(Relation rel, Predicate p) {
        Relation result = new RelationBuilder()
        .attributeNames(rel.getAttrs())
        .attributeTypes(rel.getTypes())
        .build();

        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);
            if (p.check(row)) {
                result.insert(row);
            }
        }
        return result;
    }

    // need to do
    public Relation project(Relation rel, List<String> attrs) {
        for (String attr : attrs) {
            if (!rel.hasAttr(attr)) {
                throw new IllegalArgumentException("Attribute " + attr + " does not exist in the relation.");
            }
        }
        List<Type> types = new ArrayList<>();
        for (String attr : attrs) {
            types.add(rel.getTypes().get(rel.getAttrIndex(attr)));
        }
        Relation result = new RelationBuilder()
        .attributeNames(attrs)
        .attributeTypes(types)
        .build();
        Set<List<Cell>> seenRows = new HashSet<>();
        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);
            List<Cell> projectedRow = new ArrayList<>();
            for (String attr : attrs) {
                projectedRow.add(row.get(rel.getAttrIndex(attr)));
            }
            if (!seenRows.contains(projectedRow)) {
                seenRows.add(projectedRow);
                result.insert(projectedRow);
            }
        }
        return result;
    }

    @Override
    public Relation union(Relation rel1, Relation rel2) {
        if (!compatible(rel1, rel2)) {
            throw new IllegalArgumentException("Relations are not compatible for union.");
        }
        Relation result = new RelationBuilder()
        .attributeNames(rel1.getAttrs())
        .attributeTypes(rel1.getTypes())
        .build();
        Set<List<Cell>> seenRows = new HashSet<>();
        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row = rel1.getRow(i);
            if (!seenRows.contains(row)) {
                seenRows.add(row);
                result.insert(row);
            }
        }
        for (int i = 0; i < rel2.getSize(); i++) {
            List<Cell> row = rel2.getRow(i);
            if (!seenRows.contains(row)) {
                seenRows.add(row);
                result.insert(row);
            }
        }
        return result;
    }

    @Override
    public Relation intersect(Relation rel1, Relation rel2) {
        if (!compatible(rel1, rel2)) {
            throw new IllegalArgumentException("Relations are not compatible for intersection.");
        }
        Set<List<Cell>> rows1 = new HashSet<>();
        for (int i = 0; i < rel1.getSize(); i++) {
            rows1.add(rel1.getRow(i));
        }
        Relation result = new RelationBuilder()
        .attributeNames(rel1.getAttrs())
        .attributeTypes(rel1.getTypes())
        .build();
        Set<List<Cell>> seenRows = new HashSet<>();
        for (int i = 0; i < rel2.getSize(); i++) {
            List<Cell> row = rel2.getRow(i);
            if (rows1.contains(row)) {
                if (!seenRows.contains(row)) {
                    seenRows.add(row);
                    result.insert(row);
                }
            }
        }
        return result;
    }

    @Override
    public Relation diff(Relation rel1, Relation rel2) {
        if (!compatible(rel1, rel2)) {
            throw new IllegalArgumentException("Relations are not compatible for difference.");
        }
        Relation result = new RelationBuilder()
        .attributeNames(rel1.getAttrs())
        .attributeTypes(rel1.getTypes())
        .build();
        Set<List<Cell>> rows2 = new HashSet<>();
        Set<List<Cell>> seenRows = new HashSet<>();
        for (int i = 0; i < rel2.getSize(); i++) {
            rows2.add(rel2.getRow(i));
        }
        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row = rel1.getRow(i);
            if (!rows2.contains(row)) {
                if (!seenRows.contains(row)) {
                    seenRows.add(row);
                    result.insert(row);
                }
            }
        }
        return result;
    }

    // may need to do
    @Override
    public Relation rename(Relation rel, List<String> origAttr, List<String> renamedAttr) {
        if (origAttr.size() != renamedAttr.size()) {
            throw new IllegalArgumentException("Original and renamed attribute lists must be of the same size.");
        }
        for (String attr : origAttr) {
            if (!rel.hasAttr(attr)) {
                throw new IllegalArgumentException("Attribute " + attr + " does not exist in the relation.");
            }
        }
        List<String> relAttrs = rel.getAttrs();
        List<String> finalAttrs = new ArrayList<>();
        for (int i = 0; i < relAttrs.size(); i++) {
            int idx = origAttr.indexOf(relAttrs.get(i));
            if (idx >= 0) {
                finalAttrs.add(renamedAttr.get(idx));
            } else {
                finalAttrs.add(relAttrs.get(i));
            }
        }
        Relation result = new RelationBuilder()
        .attributeNames(finalAttrs)
        .attributeTypes(rel.getTypes())
        .build();
        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);
            result.insert(row);
        }
        return result;
    
    }

    @Override
    public Relation cartesianProduct(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'cartesianProduct'");
    }

    // need to do
    public Relation join(Relation rel1, Relation rel2) {
        List<String> attrs1 = rel1.getAttrs();
        List<String> attrs2 = rel2.getAttrs();

        List<Type> types1 = rel1.getTypes();
        List<Type> types2 = rel2.getTypes();

        //finds common attributes
        List<String> commonAttrs = new  ArrayList<>();
        for (String attr : attrs1) {
            if (rel2.hasAttr(attr)) {
                commonAttrs.add(attr);
            }
        }
        //builds result schema
        List<String> newAttrs = new  ArrayList<>(attrs1);
        List<Type> newTypes = new ArrayList<>(types1);

        for (int i = 0; i < attrs2.size(); i++) {
            if (!commonAttrs.contains(attrs2.get(i))) {
                newAttrs.add(attrs2.get(i));
                newTypes.add(types2.get(i));
            }
        }
        Relation result = new RelationBuilder()
                .attributeNames(newAttrs)
                .attributeTypes(newTypes)
                .build();
        Set<List<Cell>> seenRows = new HashSet<>();
        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);

            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);

                boolean match = true;

                for (String attr : commonAttrs) {
                    int index1 = rel1.getAttrIndex(attr);
                    int index2 = rel2.getAttrIndex(attr);

                    if (!row1.get(index1).equals(row2.get(index2))) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    List<Cell> newRow = new ArrayList<>(row1);

                    for (int k = 0; k < attrs2.size(); k++) {
                        if (!commonAttrs.contains(attrs2.get(k))) {
                            newRow.add(row2.get(k));
                        }
                    }
                    if (!seenRows.contains(newRow)) {
                        seenRows.add(newRow);
                        result.insert(newRow);
                    }
                }
            }
        }
        return result;
    }

    // may need to do
    @Override
    public Relation join(Relation rel1, Relation rel2, Predicate p) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'join'");
    }


    public boolean compatible(Relation rel1, Relation rel2) {
        List<Type> types1 = rel1.getTypes();
        List<Type> types2 = rel2.getTypes();
        if (types1.size() != types2.size()) {
            return false;
        }
        for (int i = 0; i < types1.size(); i++) {
            if (types1.get(i) != types2.get(i)) {
                return false;
            }
        }
        return true;
    }


}

