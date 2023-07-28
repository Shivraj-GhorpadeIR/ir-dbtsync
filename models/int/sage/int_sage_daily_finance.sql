SELECT
   "replace"(
     ponumber:: text,
      'ERC-':: text,
      '':: text
   ) AS "replace",
  userid,
  custvendname,
  TIMEZONE ('CST', auwhencreated ) as auwhencreated
FROM
   {{ ref('stg_sage_so_document') }}
WHERE
   auwhencreated >= (getdate() - 31:: bigint)