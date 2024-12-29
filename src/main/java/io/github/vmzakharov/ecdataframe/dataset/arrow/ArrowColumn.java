package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

abstract public class ArrowColumn
{
    private final Field field;
    private final String name;

    public ArrowColumn(String name, Field field)
    {
        this.field = field;
        this.name = name;
    }

    public abstract ValueType type();

    public Field field()
    {
        return this.field;
    }

    public String name()
    {
        return this.name;
    }

    public abstract void setVectorValues(FieldVector vector, Object... values);

    public abstract void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector);

    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        throw new UnsupportedOperationException("Not supported yet for type " + this.type());
    }
}
