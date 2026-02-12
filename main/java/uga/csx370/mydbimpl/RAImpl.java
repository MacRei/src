package uga.csx370.mydbimpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import uga.csx370.mydb.Cell;
import uga.csx370.mydb.Predicate;
import uga.csx370.mydb.RA;
import uga.csx370.mydb.Relation;
import uga.csx370.mydb.RelationBuilder;
import uga.csx370.mydb.Type;

public class RAImpl implements RA {

    @Override
    public Relation select(Relation rel, Predicate p) {

        Relation result = new RelationBuilder()
                .attributeNames(rel.getAttrs())
                .attributeTypes(rel.getTypes())
                .build();

        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);

            if (p.check(row)) {
                result.insert(new ArrayList<>(row));
            }
        }

        return result;
    }

    @Override
    public Relation project(Relation rel, List<String> attrs) {
        List<String> allAttrs = rel.getAttrs();
        List<Type> allTypes = rel.getTypes();

        List<String> newAttrs = new ArrayList<>();
        List<Type> newTypes = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();

        for (String attr : attrs) {
            if (!rel.hasAttr(attr))
                throw new RuntimeException("Attribute not found: " + attr);

            int index = rel.getAttrIndex(attr);
            newAttrs.add(attr);
            newTypes.add(allTypes.get(index));
            indices.add(index);
        }

        Relation result = new RelationBuilder()
                .attributeNames(newAttrs)
                .attributeTypes(newTypes)
                .build();

        Set<List<Cell>> seen = new HashSet<>();

        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);

            List<Cell> newRow = new ArrayList<>();
            for (int index : indices) {
                newRow.add(row.get(index));
            }

            if (!seen.contains(newRow)) {
                result.insert(newRow);
                seen.add(newRow);
            }
        }

        return result;
    }

    @Override
    public Relation union(Relation rel1, Relation rel2) {

        if (!rel1.getAttrs().equals(rel2.getAttrs()))
            throw new RuntimeException("Relations not union compatible.");

        Relation result = new RelationBuilder()
                .attributeNames(rel1.getAttrs())
                .attributeTypes(rel1.getTypes())
                .build();

        Set<List<Cell>> rows = new HashSet<>();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row = rel1.getRow(i);
            result.insert(new ArrayList<>(row));
            rows.add(row);
        }

        for (int i = 0; i < rel2.getSize(); i++) {
            List<Cell> row = rel2.getRow(i);

            if (!rows.contains(row)) {
                result.insert(new ArrayList<>(row));
            }
        }

        return result;
    }

    @Override
    public Relation intersect(Relation rel1, Relation rel2) {

        Relation result = new RelationBuilder()
                .attributeNames(rel1.getAttrs())
                .attributeTypes(rel1.getTypes())
                .build();

        Set<List<Cell>> rows2 = new HashSet<>();

        for (int i = 0; i < rel2.getSize(); i++) {
            rows2.add(rel2.getRow(i));
        }

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row = rel1.getRow(i);

            if (rows2.contains(row)) {
                result.insert(new ArrayList<>(row));
            }
        }

        return result;
    }

    @Override
    public Relation diff(Relation rel1, Relation rel2) {

        Relation result = new RelationBuilder()
                .attributeNames(rel1.getAttrs())
                .attributeTypes(rel1.getTypes())
                .build();

        Set<List<Cell>> rows2 = new HashSet<>();

        for (int i = 0; i < rel2.getSize(); i++) {
            rows2.add(rel2.getRow(i));
        }

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row = rel1.getRow(i);

            if (!rows2.contains(row)) {
                result.insert(new ArrayList<>(row));
            }
        }

        return result;
    }

    @Override
    public Relation rename(Relation rel, List<String> origAttr, List<String> renamedAttr) {

        List<String> attrs = new ArrayList<>(rel.getAttrs());

        for (int i = 0; i < origAttr.size(); i++) {
            if (!rel.hasAttr(origAttr.get(i)))
                throw new RuntimeException("Attribute not found: " + origAttr.get(i));

            int index = rel.getAttrIndex(origAttr.get(i));
            attrs.set(index, renamedAttr.get(i));
        }

        Relation result = new RelationBuilder()
                .attributeNames(attrs)
                .attributeTypes(rel.getTypes())
                .build();

        for (int i = 0; i < rel.getSize(); i++) {
            result.insert(rel.getRow(i));
        }

        return result;
    }

    @Override
    public Relation cartesianProduct(Relation rel1, Relation rel2) {

        List<String> newAttrs = new ArrayList<>();
        newAttrs.addAll(rel1.getAttrs());
        newAttrs.addAll(rel2.getAttrs());

        List<Type> newTypes = new ArrayList<>();
        newTypes.addAll(rel1.getTypes());
        newTypes.addAll(rel2.getTypes());

        Relation result = new RelationBuilder()
                .attributeNames(newAttrs)
                .attributeTypes(newTypes)
                .build();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);

            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);

                List<Cell> newRow = new ArrayList<>();
                newRow.addAll(row1);
                newRow.addAll(row2);

                result.insert(newRow);
            }
        }

        return result;
    }

    @Override
    public Relation join(Relation rel1, Relation rel2) {

        List<String> attrs1 = rel1.getAttrs();
        List<String> attrs2 = rel2.getAttrs();

        List<Type> types1 = rel1.getTypes();
        List<Type> types2 = rel2.getTypes();

        List<String> common = new ArrayList<>();
        for (String attr : attrs1) {
            if (attrs2.contains(attr)) {
                common.add(attr);
            }
        }

        List<String> newAttrs = new ArrayList<>(attrs1);
        List<Type> newTypes = new ArrayList<>(types1);

        for (int i = 0; i < attrs2.size(); i++) {
            if (!common.contains(attrs2.get(i))) {
                newAttrs.add(attrs2.get(i));
                newTypes.add(types2.get(i));
            }
        }

        Relation result = new RelationBuilder()
                .attributeNames(newAttrs)
                .attributeTypes(newTypes)
                .build();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);

            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);

                boolean match = true;

                for (String attr : common) {
                    int index1 = attrs1.indexOf(attr);
                    int index2 = attrs2.indexOf(attr);

                    if (!row1.get(index1).equals(row2.get(index2))) {
                        match = false;
                        break;
                    }
                }

                if (match) {
                    List<Cell> newRow = new ArrayList<>(row1);

                    for (int k = 0; k < attrs2.size(); k++) {
                        if (!common.contains(attrs2.get(k))) {
                            newRow.add(row2.get(k));
                        }
                    }

                    result.insert(newRow);
                }
            }
        }
        return result;
    }

    @Override
    public Relation join(Relation rel1, Relation rel2, Predicate p) {

        Relation product = cartesianProduct(rel1, rel2);
        return select(product, p);
    }
}
