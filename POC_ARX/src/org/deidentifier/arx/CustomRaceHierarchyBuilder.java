package org.deidentifier.arx;
import org.deidentifier.arx.aggregates.HierarchyBuilderGroupingBased;

public class CustomRaceHierarchyBuilder extends HierarchyBuilderGroupingBased<String> {

    public CustomRaceHierarchyBuilder() {
        super(Type.ORDER_BASED, DataType.STRING);
    }

    @Override
    protected AbstractGroup[][] prepareGroups() {
        // perform manual group mapping
        String[] values = getData();

        AbstractGroup[][] result = new AbstractGroup[2][values.length];

        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            // level 0
            if (value.equals("Caucasian")) {
                result[0][i] = new SimpleGroup("Non-Minority");
            } else {
                result[0][i] = new SimpleGroup("Minority");
            }
            // level 1 (total generalization)
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
