package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDecimalColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.IntToObjectFunction;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class ArrowDecimalColumn
extends ArrowColumn
{
    public ArrowDecimalColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(new ArrowType.Decimal(1000, 10, 100)), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.DECIMAL;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (BigDecimal) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfDecimalColumn decimalColumn = (DfDecimalColumn) dfColumn;

        this.setVectorValues(decimalColumn::isNull, decimalColumn::getTypedObject, vector, decimalColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToObjectFunction<BigDecimal> valueGetter,
            FieldVector vector,
            int size)
    {
        DecimalVector decimalVector = (DecimalVector) vector;
        decimalVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                decimalVector.setNull(i);
            }
            else
            {
                decimalVector.setSafe(i, valueGetter.valueOf(i).setScale(decimalVector.getScale(), RoundingMode.HALF_UP));
            }
        }

        decimalVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        DecimalVector decimalVector = (DecimalVector) vector;

        MutableList<BigDecimal> columnValues = Lists.mutable.of();

        for (int i = 0; i < decimalVector.getValueCount(); i++)
        {
            columnValues.add(decimalVector.isNull(i) ? null : decimalVector.getObject(i));
        }

        dataFrame.addDecimalColumn(this.name(), columnValues);
    }
}
