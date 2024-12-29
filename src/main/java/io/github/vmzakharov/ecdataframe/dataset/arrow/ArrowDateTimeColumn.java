package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateTimeColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.IntToObjectFunction;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class ArrowDateTimeColumn
extends ArrowColumn
{
    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    public ArrowDateTimeColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.DATE_TIME;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (LocalDateTime) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfDateTimeColumn dateTimeColumn = (DfDateTimeColumn) dfColumn;

        this.setVectorValues(dateTimeColumn::isNull, dateTimeColumn::getTypedObject, vector, dateTimeColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToObjectFunction<LocalDateTime> valueGetter,
            FieldVector vector,
            int size)
    {
        DateMilliVector dateTimeVector = (DateMilliVector) vector;
        dateTimeVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                dateTimeVector.setNull(i);
            }
            else
            {
                dateTimeVector.setSafe(i, valueGetter.valueOf(i).atZone(UTC_ZONE_ID).toInstant().toEpochMilli());
            }
        }

        dateTimeVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        DateMilliVector dateTimeVector = (DateMilliVector) vector;

        MutableList<LocalDateTime> columnValues = Lists.mutable.of();

        for (int i = 0; i < dateTimeVector.getValueCount(); i++)
        {
            columnValues.add(dateTimeVector.isNull(i) ? null : dateTimeVector.getObject(i));
        }

        dataFrame.addDateTimeColumn(this.name(), columnValues);
    }
}
