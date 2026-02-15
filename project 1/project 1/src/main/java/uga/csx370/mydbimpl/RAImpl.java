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
        List<Type> types1 = rel1.getTypes();
        List<Type> types2 = rel2.getTypes();
        if (types1.size() != types2.size()) {
            throw new IllegalArgumentException("Relations have different number of attributes.");
        }
        for (int i = 0; i < types1.size(); i++) {
            if (types1.get(i) != types2.get(i)) {
                throw new IllegalArgumentException("Attribute types do not match at index " + i);
            }
        }
        Relation result = new RelationBuilder()
        .attributeNames(rel1.getAttrs())
        .attributeTypes(types1)
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
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'join'");
        Relation result = new Relation(rel.getAttributeNames(), rel.getAttributeTypes());
        for (List<Object> tuple1 : rel1.getTuples()) {
            for (List<Object> tuple2 : rel2.getTuples()) {
                List<Object> joinedTuple = new ArrayList<>(tuple1);
                joinedTuple.addAll(tuple2);
                result.addTuple(joinedTuple);
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