{% macro validate_window(metrics_dictionary, parent_set) %}

    {% for metric_name in parent_set %}

        {% if metrics_dictionary[metric_name]["window"] is string %}

            {% if 'day' not in metrics_dictionary[metric_name]["window"] 
                and 'week' not in metrics_dictionary[metric_name]["window"] 
                and 'month' not in metrics_dictionary[metric_name]["window"]
                and 'year' not in metrics_dictionary[metric_name]["window"] %}

                {%- do exceptions.raise_compiler_error("The window " ~ metrics_dictionary[metric_name]["window"] ~ " contains a non-supported window time. Currently supported intervals are days, weeks, months, and years.") %}
            {% endif %}
        {% endif %}

    {% endfor %}

{% endmacro %}