SELECT
        c.name as company_name,
        concat(o.firstname, concat(' ',  o.lastname)) as company_owner,
        c.revive_reassignment_date,
        c.id as company_id
    FROM hubspot.companies c
    LEFT JOIN hubspot.owners o on c.hubspot_owner_id = o.id
    WHERE
    c.notes_last_updated < '2022-12-23'
    AND 
    c.createdate > '2022-09-01' AND c.createdate < '2022-11-23'
    AND
    c.num_associated_deals is null
    AND
    c.num_associated_contacts <= 1
    AND
    c.name not ilike '%test%' AND c.name not ilike '%ir57%'
    AND
    c.hubspot_owner_assigneddate < '2022-12-23'
    AND
    c.revive_reassignment_date is null
    limit 3178