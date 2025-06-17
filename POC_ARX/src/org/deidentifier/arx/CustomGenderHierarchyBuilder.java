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
            // nível 0 - Generalizar tudo como "Person"
            result[0][i] = new SimpleGroup("Person");

            // nível 1 - Generalização completa "*"
            result[1][i] = new SimpleGroup("*");
        }

        return result;
    }
    public static class SimpleGroup extends AbstractGroup {
        public SimpleGroup(String label) {
            super(label);
        }
    }
}
