package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;

import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowDataSetSchema
{
    private final MutableList<ArrowColumn> columns = Lists.mutable.empty();

    public ArrowDataSetSchema addColumn(String name, ValueType type)
    {
        this.columns.add(
            switch (type)
            {
                case INT -> new ArrowIntColumn(name);
                case LONG -> new ArrowLongColumn(name);
                case FLOAT -> new ArrowFloatColumn(name);
                case DOUBLE -> new ArrowDoubleColumn(name);
                case DECIMAL -> new ArrowDecimalColumn(name);
                case DATE -> new ArrowDateColumn(name);
                case DATE_TIME -> new ArrowDateTimeColumn(name);
                case STRING -> new ArrowStringColumn(name);
                case BOOLEAN -> new ArrowBooleanColumn(name);
                default -> throw new UnsupportedOperationException("Unsupported type for Arrow conversion: " + type);
            }
        );

        return this;
    }

    public ListIterable<ArrowColumn> columns()
    {
        return this.columns;
    }

    public Schema createArrowSchema()
    {
        return new Schema(this.columns.collect(ArrowColumn::field));
    }

    public ArrowColumn column(String columnName)
    {
        return this.columns.select(arrowColumn -> arrowColumn.name().equals(columnName)).getFirst();
    }
}
