{%- macro gen_group_by(grain, dimensions, calendar_dimensions, relevant_periods) -%}
    {{ return(adapter.dispatch('gen_group_by', 'metrics')(grain, dimensions, calendar_dimensions, relevant_periods)) }}
{%- endmacro -%}

{%- macro default__gen_group_by(grain, dimensions, calendar_dimensions, relevant_periods) -%}

{#- This model exclusively exists because dynamic group by counts based on range 
were too funky when we hardcoded values for 1+1. So we're getting around it by
making it its own function -#}

{#- The issue arises when we have an initial date column (ie date_month) where month 
is also included in the relevent periods. This causes issues and so we need to
remove the grain from the list of relevant periods so it isnt double counted -#}

    {%- set total_dimension_count = metrics.get_total_dimension_count(grain, dimensions, calendar_dimensions, relevant_periods) -%}

    {%- if grain -%}
        group by {% for number in range(1,total_dimension_count+1) -%}{{ number }}{%- if not loop.last -%}, {% endif -%}
        {%- endfor -%}
    {%- else -%}
        {%- if total_dimension_count > 0 -%}
            group by {% for number in range(1,total_dimension_count+1) -%}{{ number }} {%- if not loop.last -%}, {% endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endif -%}

{%- endmacro -%}
