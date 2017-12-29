select p.name as 'User Name ',rl.name as 'Role Name',p.create_date ,p.type_desc from sys.database_principals p
inner join sys.database_role_members rm on p.principal_id=rm.member_principal_id
inner join sys.database_principals rl on rm.role_principal_id = rl.principal_id 
order by 1



SELECT  CASE WHEN perm.state <> 'W' THEN perm.state_desc ELSE 'GRANT' END
    + SPACE(1) + perm.permission_name + SPACE(1)
    + SPACE(1) + 'TO' + SPACE(1) + usr.name COLLATE database_default
    + CASE WHEN perm.state <> 'W' THEN SPACE(0) ELSE SPACE(1) + 'WITH GRANT OPTION' END AS '--Database Level Permissions'
FROM    sys.database_permissions AS perm
    INNER JOIN
    sys.database_principals AS usr
    ON perm.grantee_principal_id = usr.principal_id
--WHERE   usr.name = 'EtlUser'
AND perm.major_id = 0
ORDER BY perm.permission_name ASC, perm.state_desc ASC




SELECT  

 /* CASE WHEN perm.state <> 'W' THEN perm.state_desc ELSE 'GRANT' END
    + SPACE(1) + perm.permission_name + SPACE(1) + 'ON ' + QUOTENAME(USER_NAME(obj.schema_id)) + '.' + QUOTENAME(obj.name)
    + CASE WHEN cl.column_id IS NULL THEN SPACE(0) ELSE '(' + QUOTENAME(cl.name) + ')' END
    + SPACE(1) + 'TO' + SPACE(1) +  usr.name  COLLATE database_default
    + CASE WHEN perm.state <> 'W' THEN SPACE(0) ELSE SPACE(1) + 'WITH GRANT OPTION' END AS '--Object Level Permissions', 
*/	
	perm.permission_name,QUOTENAME(USER_NAME(obj.schema_id)) + '.' + QUOTENAME(obj.name), usr.name 
FROM    sys.database_permissions AS perm
    INNER JOIN
    sys.objects AS obj
    ON perm.major_id = obj.[object_id]
    INNER JOIN
    sys.database_principals AS usr
    ON perm.grantee_principal_id = usr.principal_id
    LEFT JOIN
    sys.columns AS cl
    ON cl.column_id = perm.minor_id AND cl.[object_id] = perm.major_id
--WHERE   usr.name = 'EtlUser'
ORDER BY  usr.name --perm.permission_name ASC, perm.state_desc ASC


