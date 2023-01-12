{%- macro get_model_group(models_grouping, metric_model, metric_model_name, metric_name, metric_timestamp=none, metric_filters=none, metric_window=none) -%}

{#- 
This macro is called from get_models_grouping in order to calculate
the group for each model based on the inputs. This allows us to reduce
the complexity of the aforementioned macro because there is a factorial 
combination of possibilities based on the inputs, minus some combinations
that are invalid.

By factorial, we mean that the three potential inputs can be combined in 
a multitude of different ways in order to calculate the group. The potential 
combinations are:
    - timestamp
    - filters
    - timestamp + window
    - timestamp + filters
    - timestamp + filters + window
 -#}

    {% set metric_model_list = [metric_model_name] %}

    {% if metric_timestamp %}
        {% set timestamp_list = [
            metric_timestamp | lower
        ]%}
    {% else %}
        {% set timestamp_list = [] %}
    {% endif %}

    {% if metric_window %}
        {% set window_list = [
                metric_window.count | lower
                ,metric_window.period | lower
            ]%}
    {% else %}
        {% set window_list = [] %}
    {% endif %}

    {% if metric_filters %}
        {% set filter_list = [] %}
        {% for filter in metric_filters %}
            {% do filter_list.append(filter.field | lower)%}
            {% do filter_list.append(filter.operator | lower)%}
            {% do filter_list.append(filter.value | lower)%}
        {% endfor %}
    {% else %}
        {% set filter_list = [] %}
    {% endif %}

    {% set group_list = (metric_model_list + timestamp_list + window_list + filter_list) | sort %}
    {% set group_name = 'model_' ~ local_md5(group_list | join('_')) %}

    {% if not models_grouping[group_name] %}
        {% do models_grouping.update({group_name:{}})%}
        {% do models_grouping[group_name].update({'metric_names':{}})%}
        {% do models_grouping[group_name].update({'metric_model':metric_model})%}
        {% do models_grouping[group_name].update({'timestamp':metric_timestamp})%}
        {% do models_grouping[group_name].update({'filters':metric_filters})%}
        {% do models_grouping[group_name].update({'window':metric_window})%}
        {% do models_grouping[group_name].update({'metric_names':[metric_name]})%}
    {% else %}
        {% set metric_names = models_grouping[group_name]['metric_names'] %}
        {% do metric_names.append(metric_name)%}
        {% do models_grouping[group_name].update({'metric_names':metric_names})%}
    {% endif %}

    {% do return(metrics_grouping) %}

{%- endmacro -%}