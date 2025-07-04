package org.deidentifier.arx;
import org.deidentifier.arx.aggregates.HierarchyBuilderGroupingBased;

// for sensitive attributes and quasi-identifiers, I build custom hierarchies for import into the Main module
public class CustomBloodTypeHierarchyBuilder extends HierarchyBuilderGroupingBased<String> {

    // hierarquia de generalização personalizada para o tipo de sangue
    public CustomBloodTypeHierarchyBuilder() {

        super(Type.ORDER_BASED, DataType.STRING); // the hierarchy type is defined explicitly, with manual value ordering to prevent ARX from applying automatic sorting
    }

    @Override
    protected AbstractGroup[][] prepareGroups() {
        String[] values = getData(); // ex: ["A+", "A−", ..., "O−"]
        AbstractGroup[][] result = new AbstractGroup[2][values.length];

        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            // Level 0 – aggregation based on primary type
            if (value.startsWith("A") && !value.startsWith("AB")) {
                result[0][i] = new SimpleGroup("A");
            } else if (value.startsWith("B") && !value.startsWith("AB")) {
                result[0][i] = new SimpleGroup("B");
            } else if (value.startsWith("AB")) {
                result[0][i] = new SimpleGroup("AB");
            } else if (value.startsWith("O")) {
                result[0][i] = new SimpleGroup("O");
            } else {
                result[0][i] = new SimpleGroup("Other");
            }

            // level 1 – Fully generalized
            result[1][i] = new SimpleGroup("*");
        }

        return result;
    }
    // its purpose is solely to label a generalization group (e.g., A, B, and *)
    private static class SimpleGroup extends AbstractGroup {
        private SimpleGroup(String label) {
            super(label);
        }
    }
}
