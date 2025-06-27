package org.deidentifier.arx;
import org.deidentifier.arx.aggregates.HierarchyBuilderGroupingBased;

public class CustomRaceHierarchyBuilder extends HierarchyBuilderGroupingBased<String> {

    public CustomRaceHierarchyBuilder() {
        super(Type.ORDER_BASED, DataType.STRING);
    }

    @Override
    protected AbstractGroup[][] prepareGroups() {
        // mapear grupos manualmente
        String[] values = getData();

        AbstractGroup[][] result = new AbstractGroup[2][values.length];

        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            // Nível 0
            if (value.equals("Caucasian")) {
                result[0][i] = new SimpleGroup("Non-Minority");
            } else {
                result[0][i] = new SimpleGroup("Minority");
            }
            // nível 1 (generalização total)
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
