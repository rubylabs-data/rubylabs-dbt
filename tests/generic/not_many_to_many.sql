{% test not_one_to_many(model, column_name, field, to=model) %}

{% if to == model %}

select {{column_name}}, count(distinct {{field}}) count 
from {{model}}
group by 1 
having count > 1

{% else %}

select {{column_name}}, count(distinct b.{{field}}) count 
from {{model}} a
join {{to}} b on a.{{column_name}} = b.{{column_name}}
group by 1 
having count > 1

{% endif %}

{% endtest %}