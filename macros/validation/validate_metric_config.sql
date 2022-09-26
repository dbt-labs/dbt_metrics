{%- macro validate_metric_config(metrics_dictionary) -%}

    {#- We loop through the metrics dictionary here to ensure that
    1) all configs are real configs we know about
    2) all of those have valid values passed
    returned or used, not just those listed -#}

    {%- set accepted_configs = {
        "enabled" : {"accepted_values" : [True, False]},
        "treat_null_values_as_zero" : {"accepted_values" : [True, False]}
        }
    -%}

    {%- for metric in metrics_dictionary -%}
        {%- set metric_config = metrics_dictionary[metric].get("config", None) -%}
        {%- if metric_config -%}
            {%- for config in metric_config -%}
                {%- set config_value = metric_config[config] -%}
                {#- some wonkiness here -- metric_config is not a dictionary, it's a MetricConfig object, so can't use the items() method -#}
                {#- check that the config is one that we expect -#}
                {%- if not accepted_configs[config] -%}
                    {%- do exceptions.raise_compiler_error("The metric " ~ metric ~ " has an invalid config option. The config '" ~ config ~ "' is not accepted.") -%}
                {%- endif -%}
                {#- check that the config datatype is expected -#}
                {%- if accepted_configs[config] -%}
                    {%- set accepted_values = accepted_configs[config]["accepted_values"] -%}
                    {%- if not config_value in accepted_values -%}
                        {%- do exceptions.raise_compiler_error("The metric " ~ metric ~ " has an invalid config value specified. The config '" ~ config ~ "' expects one of " ~ accepted_values) -%}
                    {%- endif -%}
                {% endif %}
            {%- endfor %}
        {%- endif -%}
    {%- endfor %}
            


{%- endmacro -%}