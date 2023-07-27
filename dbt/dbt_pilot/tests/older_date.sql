SELECT
*
FROM
{{ ref('test_pos_shop') }}
WHERE division = 'MAC'