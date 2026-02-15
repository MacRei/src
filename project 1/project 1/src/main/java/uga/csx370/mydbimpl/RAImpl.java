package uga.csx370.mydbimpl;

import java.util.ArrayList;
import java.util.List;

import uga.csx370.mydb.Cell;
import uga.csx370.mydb.Predicate;
import uga.csx370.mydb.RA;
import uga.csx370.mydb.Relation;
import uga.csx370.mydb.RelationBuilder;
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
        // validate attributes exist
        for (String attr : attrs) {
            if (!rel.hasAttr(attr)) {
                throw new IllegalArgumentException("Attribute not found: " + attr);
            }
        }
        // build projected types list
        List<Type> projectedTypes = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        for (String attr : attrs) {
            int idx = rel.getAttrIndex(attr);
            indices.add(idx);
            projectedTypes.add(rel.getTypes().get(idx));
        }
        Relation result = new RelationBuilder()
                .attributeNames(attrs)
                .attributeTypes(projectedTypes)
                .build();
        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);
            List<Cell> projectedRow = new ArrayList<>();
            for (int idx : indices) {
                projectedRow.add(row.get(idx));
            }
            result.insert(projectedRow);
        }
        return result;
    }

    @Override
    public Relation union(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'union'");
    }

    @Override
    public Relation intersect(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'intersect'");
    }

    @Override
    public Relation diff(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'diff'");
    }

    // may need to do
    @Override
    public Relation rename(Relation rel, List<String> origAttr, List<String> renamedAttr) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'rename'");
    }

    @Override
    public Relation cartesianProduct(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'cartesianProduct'");
    }

    // need to do
    public Relation join(Relation rel1, Relation rel2) {
        // find common attributes
        List<String> commonAttrs = new ArrayList<>();
        for (String attr : rel1.getAttrs()) {
            if (rel2.hasAttr(attr)) {
                commonAttrs.add(attr);
            }
        }
        // build result attribute list: all of rel1, then rel2 attrs that aren't common
        List<String> resultAttrs = new ArrayList<>(rel1.getAttrs());
        List<Type> resultTypes = new ArrayList<>(rel1.getTypes());
        List<Integer> rel2NonCommonIndices = new ArrayList<>();
        for (int i = 0; i < rel2.getAttrs().size(); i++) {
            String attr = rel2.getAttrs().get(i);
            if (!commonAttrs.contains(attr)) {
                resultAttrs.add(attr);
                resultTypes.add(rel2.getTypes().get(i));
                rel2NonCommonIndices.add(i);
            }
        }
        Relation result = new RelationBuilder()
                .attributeNames(resultAttrs)
                .attributeTypes(resultTypes)
                .build();
        // perform the natural join
        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);
                // check if common attributes match
                boolean match = true;
                for (String attr : commonAttrs) {
                    Cell val1 = row1.get(rel1.getAttrIndex(attr));
                    Cell val2 = row2.get(rel2.getAttrIndex(attr));
                    if (!val1.equals(val2)) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    List<Cell> joinedRow = new ArrayList<>(row1);
                    for (int idx : rel2NonCommonIndices) {
                        joinedRow.add(row2.get(idx));
                    }
                    result.insert(joinedRow);
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

}