package uga.csx370.mydbimpl;

import java.util.List;

import uga.csx370.mydb.Predicate;
import uga.csx370.mydb.RA;
import uga.csx370.mydb.Relation;

public class RAImpl implements RA {

    // need to do
    public Relation select(Relation rel, Predicate p) {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'select'");
        Relation result = new Relation(rel.getAttributeNames(), rel.getAttributeTypes());
        for (List<Object> tuple : rel.getTuples()) {
            if (p.evaluate(tuple)) {
                result.addTuple(tuple);
            }
        }
        return result;
    }

    // need to do
    public Relation project(Relation rel, List<String> attrs) {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'project'");
        Relation result = new Relation(attrs, rel.getAttributeTypes());
        for (List<Object> tuple : rel.getTuples()) {
            List<Object> projectedTuple = new ArrayList<>();
            for (String attr : attrs) {
                int index = rel.getAttributeNames().indexOf(attr);
                projectedTuple.add(tuple.get(index));
            }
            result.addTuple(projectedTuple);
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