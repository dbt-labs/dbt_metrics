{% macro validate_lookback(metrics_dictionary, parent_set) %}

    {% for metric_name in parent_set %}

        {% if metrics_dictionary[metric_name]["lookback"] is string %}

            {% if 'day' not in metrics_dictionary[metric_name]["lookback"] 
                and 'week' not in metrics_dictionary[metric_name]["lookback"] 
                and 'month' not in metrics_dictionary[metric_name]["lookback"]
                and 'year' not in metrics_dictionary[metric_name]["lookback"] %}
                
                {%- do exceptions.raise_compiler_error("The lookback " ~ metrics_dictionary[metric_name]["lookback"] ~ " contains a non-supported lookback time. Currently supported intervals are days, weeks, months, and years.") %}
            {% endif %}
        {% endif %}

    {% endfor %}

{% endmacro %}