with study_results as (
 
    {% for study_id in ['XB002-101','XL092-002','XB010-101','XL092-303','XL092-304','XL092-305','XL092-009','XL495-101','XL309-101','XB628-101','XL092-311','XB371-101','XL092-201'] %}

    {{ get_study_kpi_results_mart(study_id) }}
    {% if loop.index < 13 %}
    union all
    {% endif %}
    {% endfor %}
 
)
 
select * from study_results