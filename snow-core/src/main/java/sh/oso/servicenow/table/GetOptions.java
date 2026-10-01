package sh.oso.servicenow.table;

import java.util.List;

/** Options for {@link TableApiClient#get}. */
public record GetOptions(
        List<String> fields, DisplayValue displayValue, boolean excludeReferenceLink) {

    public GetOptions {
        fields = fields == null ? List.of() : List.copyOf(fields);
        displayValue = displayValue == null ? DisplayValue.FALSE : displayValue;
    }

    public static GetOptions defaults() {
        return new GetOptions(List.of(), DisplayValue.FALSE, true);
    }

    public GetOptions withFields(List<String> fields) {
        return new GetOptions(fields, displayValue, excludeReferenceLink);
    }
}
