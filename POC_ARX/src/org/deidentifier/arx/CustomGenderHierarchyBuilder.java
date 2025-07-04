package org.deidentifier.arx;
import org.deidentifier.arx.aggregates.HierarchyBuilderGroupingBased;

public class CustomGenderHierarchyBuilder extends HierarchyBuilderGroupingBased<String> {

    public CustomGenderHierarchyBuilder() {
        super(Type.ORDER_BASED, DataType.STRING);
    }

    @Override
    protected AbstractGroup[][] prepareGroups() {
        String[] values = getData();
        AbstractGroup[][] result = new AbstractGroup[2][values.length];
        for (int i = 0; i < values.length; i++) {
            // level 0 – generalize everything as "Person"
            result[0][i] = new SimpleGroup("Person");

            // level 1 – complete generalization represented by "*"
            result[1][i] = new SimpleGroup("*");
        }

        return result;
    }
    private static class SimpleGroup extends AbstractGroup {
        private SimpleGroup(String label) {
            super(label);
        }
    }
}
