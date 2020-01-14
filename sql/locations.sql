/*
 This script generates an Iniz-compliant import CSV from a pre-existing database
 */

select      l.uuid as 'Uuid',
            l.retired as 'Void/Retire',
            l.name as 'Name',
            l.description as 'Description',
            p.uuid as 'Parent'
from        location l
left join   location p on l.parent_location = p.location_id
order by    l.location_id asc;
