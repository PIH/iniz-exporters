select      t.uuid as 'Uuid',
            t.retired as 'Void/Retire',
            t.name as 'Name',
            t.description as 'Description',
            t.java_class_name as 'Java class name',
            p.uuid as 'Parent'
from        order_type t
left join   order_type p on t.parent = p.order_type_id
order by    t.order_type_id asc;
