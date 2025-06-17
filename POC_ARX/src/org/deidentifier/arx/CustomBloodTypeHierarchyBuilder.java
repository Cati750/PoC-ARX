package org.deidentifier.arx;
import org.deidentifier.arx.aggregates.HierarchyBuilderGroupingBased;

//para atributos sensíveis e quase-identificadores construo hierarquias personalizadas para importação na Main
public class CustomBloodTypeHierarchyBuilder extends HierarchyBuilderGroupingBased<String> {

    // hierarquia de generalização personalizada para o tipo de sangue
    public CustomBloodTypeHierarchyBuilder() {

        super(Type.ORDER_BASED, DataType.STRING); // defino o tipo de hirarquia, mas defino manualmente como os valores se ordenam (para o ARX não criar uma ordem automática)
    }

    @Override
    protected AbstractGroup[][] prepareGroups() {
        String[] values = getData(); // ex: ["A+", "A−", ..., "O−"]
        AbstractGroup[][] result = new AbstractGroup[2][values.length];

        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            // nível 0 - agrupamento por tipo principal
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

            // nível 1 - totalmente generalizado
            result[1][i] = new SimpleGroup("*");
        }

        return result;
    }
    // serve apenas para dar um nome a um grupo de generalização (como A, B e *)
    public static class SimpleGroup extends AbstractGroup {
        public SimpleGroup(String label) {
            super(label);
        }
    }
}
